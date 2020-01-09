package publish

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/lbryio/reflector.go/reflector"

	mediainfo "github.com/lbryio/go_mediainfo"
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/lbrycrd"
	"github.com/lbryio/lbry.go/v2/stream"
	pb "github.com/lbryio/types/v2/go"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/golang/protobuf/proto"
	"golang.org/x/crypto/sha3"
)

var TODO = `
	import cert from wallet
	get all utxos from chainquery
	create transaction
	sign it with the channel
	track state of utxos across publishes from this channel so that we can just do one query to get utxos
	prioritize only confirmed utxos

	Handling all the issues we handle currently with lbrynet:
		"Couldn't find private key for id",
		"You already have a stream claim published under the name",
		"Cannot publish using channel",
		"txn-mempool-conflict",
		"too-long-mempool-chain",
		"Missing inputs",
		"Not enough funds to cover this transaction",
}
`

func Publish(client *lbrycrd.Client, path, name, address string, details Details, reflectorAddress string) (*wire.MsgTx, *chainhash.Hash, error) {
	if name == "" {
		return nil, nil, errors.Err("name required")
	}

	//TODO: sign claim if publishing into channel

	addr, err := btcutil.DecodeAddress(address, &lbrycrd.MainNetParams)
	if errors.Is(err, btcutil.ErrUnknownAddressType) {
		return nil, nil, errors.Err(`unknown address type. here's what you need to make this work:
- deprecatedrpc=validateaddress" and "deprecatedrpc=signrawtransaction" in your lbrycrd.conf
- github.com/btcsuite/btcd pinned to hash 306aecffea32
- github.com/btcsuite/btcutil pinned to 4c204d697803
- github.com/lbryio/lbry.go/v2 (make sure you have v2 at the end)`)
	}
	if err != nil {
		return nil, nil, err
	}

	amount := 0.01
	changeAddr := addr // TODO: fix this? or maybe its fine?
	tx, err := baseTx(client, amount, changeAddr)
	if err != nil {
		return nil, nil, err
	}

	claim, st, err := makeClaimAndStream(path, details)
	if err != nil {
		return nil, nil, err
	}

	err = addClaimToTx(tx, claim, name, amount, addr)
	if err != nil {
		return nil, nil, err
	}

	// sign and send
	signedTx, allInputsSigned, err := client.SignRawTransaction(tx)
	if err != nil {
		return nil, nil, err
	}
	if !allInputsSigned {
		return nil, nil, errors.Err("not all inputs for the tx could be signed")
	}

	err = reflect(st, reflectorAddress)
	if err != nil {
		return nil, nil, err
	}

	txid, err := client.SendRawTransaction(signedTx, false)
	if err != nil {
		return nil, nil, err
	}

	return signedTx, txid, nil
}

//TODO: lots of assumptions. hardcoded values need to be passed in or calculated
func baseTx(client *lbrycrd.Client, amount float64, changeAddress btcutil.Address) (*wire.MsgTx, error) {
	txFee := 0.0002 // TODO: estimate this better?

	inputs, total, err := coinChooser(client, amount+txFee)
	if err != nil {
		return nil, err
	}

	change := total - amount - txFee

	// create base raw tx
	addresses := make(map[btcutil.Address]btcutil.Amount)
	//changeAddr, err := client.GetNewAddress("")
	changeAmount, err := btcutil.NewAmount(change)
	if err != nil {
		return nil, err
	}
	addresses[changeAddress] = changeAmount
	lockTime := int64(0)
	return client.CreateRawTransaction(inputs, addresses, &lockTime)
}

func coinChooser(client *lbrycrd.Client, amount float64) ([]btcjson.TransactionInput, float64, error) {
	utxos, err := client.ListUnspentMin(1)
	if err != nil {
		return nil, 0, err
	}

	sort.Slice(utxos, func(i, j int) bool { return utxos[i].Amount < utxos[j].Amount })

	var utxo btcjson.ListUnspentResult
	for _, u := range utxos {
		if u.Spendable && u.Amount >= amount {
			utxo = u
			break
		}
	}
	if utxo.TxID == "" {
		return nil, 0, errors.Err("not enough utxos to create tx")
	}

	return []btcjson.TransactionInput{{Txid: utxo.TxID, Vout: utxo.Vout}}, utxo.Amount, nil
}

func addClaimToTx(tx *wire.MsgTx, claim *pb.Claim, name string, amount float64, claimAddress btcutil.Address) error {
	claimBytes, err := proto.Marshal(claim)
	if err != nil {
		return err
	}
	claimBytes = append([]byte{0}, claimBytes...) // version 0 = no channel sig

	amt, err := btcutil.NewAmount(amount)
	if err != nil {
		return err
	}

	script, err := getClaimPayoutScript(name, claimBytes, claimAddress)
	if err != nil {
		return err
	}

	tx.AddTxOut(wire.NewTxOut(int64(amt), script))
	return nil
}

func Decode(client *lbrycrd.Client, tx *wire.MsgTx) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, tx.SerializeSize()))
	if err := tx.Serialize(buf); err != nil {
		return "", errors.Err(err)
	}
	//txHex := hex.EncodeToString(buf.Bytes())
	//spew.Dump(txHex)
	decoded, err := client.DecodeRawTransaction(buf.Bytes())
	if err != nil {
		return "", err
	}

	data, err := json.MarshalIndent(decoded, "", "  ")
	return string(data), err
}

func reflect(st stream.Stream, reflectorAddress string) error {
	// upload blobs to reflector
	c := reflector.Client{}
	err := c.Connect(reflectorAddress)
	if err != nil {
		return errors.Err(err)
	}
	for i, b := range st {
		if i == 0 {
			err = c.SendSDBlob(b)
		} else {
			err = c.SendBlob(b)
		}
		if err != nil {
			return errors.Err(err)
		}
	}
	return nil
}

type Details struct {
	Title       string
	Description string
	Author      string
	Tags        []string
	ReleaseTime int64
}

func makeClaimAndStream(path string, details Details) (*pb.Claim, stream.Stream, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, errors.Err(err)
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, nil, errors.Err(err)
	}
	s, err := stream.New(data)
	if err != nil {
		return nil, nil, errors.Err(err)
	}

	// make the claim
	sdBlob := &stream.SDBlob{}
	err = sdBlob.FromBlob(s[0])
	if err != nil {
		return nil, nil, errors.Err(err)
	}

	filehash := sha3.Sum384(data)

	streamPB := &pb.Stream{
		Author:      details.Author,
		ReleaseTime: details.ReleaseTime,
		Source: &pb.Source{
			SdHash: s[0].Hash(),
			Name:   filepath.Base(file.Name()),
			Size:   uint64(len(data)),
			Hash:   filehash[:],
		},
	}

	mimeType, category := guessMimeType(filepath.Ext(file.Name()))
	streamPB.Source.MediaType = mimeType

	switch category {
	case "video":
		t, err := streamVideoMetadata(path)
		if err != nil {
			return nil, nil, err
		}
		streamPB.Type = t
	case "audio":
		streamPB.Type = &pb.Stream_Audio{}
	case "image":
		streamPB.Type = &pb.Stream_Image{}
	}

	claim := &pb.Claim{
		Title:       details.Title,
		Description: details.Description,
		Type:        &pb.Claim_Stream{Stream: streamPB},
	}

	return claim, s, nil
}

func getClaimPayoutScript(name string, value []byte, address btcutil.Address) ([]byte, error) {
	//OP_CLAIM_NAME <name> <value> OP_2DROP OP_DROP OP_DUP OP_HASH160 <address> OP_EQUALVERIFY OP_CHECKSIG

	pkscript, err := txscript.PayToAddrScript(address)
	if err != nil {
		return nil, errors.Err(err)
	}

	return txscript.NewScriptBuilder().
		AddOp(txscript.OP_NOP6).  //OP_CLAIM_NAME
		AddData([]byte(name)).    //<name>
		AddData(value).           //<value>
		AddOp(txscript.OP_2DROP). //OP_2DROP
		AddOp(txscript.OP_DROP).  //OP_DROP
		AddOps(pkscript).         //OP_DUP OP_HASH160 <address> OP_EQUALVERIFY OP_CHECKSIG
		Script()
}

func streamVideoMetadata(path string) (*pb.Stream_Video, error) {
	mi, err := mediainfo.GetMediaInfo(path)
	if err != nil {
		return nil, err
	}
	return &pb.Stream_Video{
		Video: &pb.Video{
			Duration: uint32(mi.General.Duration / 1000),
			Height:   uint32(mi.Video.Height),
			Width:    uint32(mi.Video.Width),
		},
	}, nil
}
