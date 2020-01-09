package cmd

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/lbrycrd"
	"github.com/lbryio/lbry.go/v2/stream"
	pb "github.com/lbryio/types/v2/go"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "publish FILE",
		Short: "Publish a file",
		Args:  cobra.ExactArgs(1),
		Run:   publishCmd,
	}
	rootCmd.AddCommand(cmd)
}

func publishCmd(cmd *cobra.Command, args []string) {
	client, err := lbrycrd.NewWithDefaultURL()
	checkErr(err)

	// make the stream
	path := args[0]
	file, err := os.Open(path)
	checkErr(err)
	data, err := ioutil.ReadAll(file)
	checkErr(err)
	s, err := stream.New(data)
	checkErr(err)

	// make the claim
	sdBlob := &stream.SDBlob{}
	err = sdBlob.FromBlob(s[0])
	checkErr(err)
	streamClaim := &pb.Claim{
		Title:       "",
		Description: "",
		Type: &pb.Claim_Stream{
			Stream: &pb.Stream{
				Type: &pb.Stream_Video{
					Video: &pb.Video{},
				},
				Source: &pb.Source{
					SdHash: s[0].Hash(),
					Name:   filepath.Base(file.Name()),
					Size:   uint64(len(data)),
					//Hash: // hash of the file itself
				},
			},
		},
	}
	rawClaim, err := proto.Marshal(streamClaim)
	checkErr(err)
	rawClaim = append([]byte{0}, rawClaim...) // version 0 = no channel sig

	//TODO: sign claim if publishing into channel

	// find utxos
	utxos, err := client.ListUnspentMin(1)
	checkErr(err)
	sort.Slice(utxos, func(i, j int) bool { return utxos[i].Amount < utxos[j].Amount })
	var output btcjson.ListUnspentResult
	for _, u := range utxos {
		if u.Spendable && u.Amount >= 0.001 {
			output = u
			break
		}
	}
	if output.TxID == "" {
		panic("Not enough spendable outputs to create tx")
	}
	inputs := []btcjson.TransactionInput{{Txid: output.TxID, Vout: output.Vout}}
	claimAmount := 0.01
	fee := 0.0002
	change := output.Amount - claimAmount - fee

	// create base raw tx
	addresses := make(map[btcutil.Address]btcutil.Amount)
	//changeAddress, err := client.GetNewAddress("")
	changeAddress, err := btcutil.DecodeAddress("bSzpgkTnAoiT2YAhUShPpfpajPESfNXVTu", &lbrycrd.MainNetParams)
	if errors.Is(err, btcutil.ErrUnknownAddressType) {
		panic(`unknown address type. here's what you need to make this work:
- deprecatedrpc=validateaddress" and "deprecatedrpc=signrawtransaction" in your lbrycrd.conf
- github.com/btcsuite/btcd pinned to hash 306aecffea32
- github.com/btcsuite/btcutil pinned to 4c204d697803
- github.com/lbryio/lbry.go/v2 (make sure you have v2 at the end)`)
	}
	checkErr(err)
	changeAmount, err := btcutil.NewAmount(change)
	checkErr(err)
	addresses[changeAddress] = changeAmount
	lockTime := int64(0)
	tx, err := client.CreateRawTransaction(inputs, addresses, &lockTime)
	checkErr(err)

	// add claim to tx
	amount, err := btcutil.NewAmount(claimAmount)
	checkErr(err)
	script, err := getClaimPayoutScript("4UC2RPcook", rawClaim, changeAddress)
	checkErr(err)
	tx.AddTxOut(wire.NewTxOut(int64(amount), script))

	// sign and send
	signedTx, allInputsSigned, err := client.SignRawTransaction(tx)
	checkErr(err)
	if !allInputsSigned {
		panic("Not all inputs for the tx could be signed!")
	}

	spew.Dump(streamClaim, signedTx)

	buf := bytes.NewBuffer(make([]byte, 0, signedTx.SerializeSize()))
	if err := tx.Serialize(buf); err != nil {
		panic(err)
	}
	txHex := hex.EncodeToString(buf.Bytes())
	spew.Dump(txHex)
	decoded, err := client.DecodeRawTransaction(buf.Bytes())
	checkErr(err)
	spew.Dump(decoded)
	//txid, err := client.SendRawTransaction(signedTx, false)
	//checkErr(err)
	//fmt.Println(txid.String())
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
