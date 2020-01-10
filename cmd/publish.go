package cmd

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	mediainfo "github.com/lbryio/go_mediainfo"
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/lbrycrd"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/lbryio/reflector.go/reflector"
	pb "github.com/lbryio/types/v2/go"
	"golang.org/x/crypto/sha3"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "publish FILE",
		Short: "Publish a file",
		Args:  cobra.ExactArgs(1),
		Run:   publishCmd,
	}
	cmd.Flags().String("name", "", "Claim name")
	cmd.Flags().String("title", "", "Title of the content")
	cmd.Flags().String("description", "", "Description of the content")
	cmd.Flags().String("author", "", "Content author")
	cmd.Flags().String("tags", "", "Comma-separated list of tags")
	cmd.Flags().Int64("release-time", 0, "original public release of content, seconds since UNIX epoch")
	rootCmd.AddCommand(cmd)
}

func publishCmd(cmd *cobra.Command, args []string) {
	claimName := mustGetFlagString(cmd, "name")
	if claimName == "" {
		log.Errorln("--name required")
		return
	}

	client, err := lbrycrd.NewWithDefaultURL()
	checkErr(err)

	// make the stream
	path := args[0]
	claim, st, err := claimAndStreamForFile(cmd, path)
	checkErr(err)
	claimBytes, err := proto.Marshal(claim)
	checkErr(err)
	claimBytes = append([]byte{0}, claimBytes...) // version 0 = no channel sig

	//TODO: sign claim if publishing into channel

	// find utxos
	utxos, err := client.ListUnspentMin(1)
	checkErr(err)
	sort.Slice(utxos, func(i, j int) bool { return utxos[i].Amount < utxos[j].Amount })
	var utxo btcjson.ListUnspentResult
	for _, u := range utxos {
		if u.Spendable && u.Amount >= 0.001 {
			utxo = u
			break
		}
	}
	if utxo.TxID == "" {
		panic("Not enough spendable outputs to create tx")
	}
	inputs := []btcjson.TransactionInput{{Txid: utxo.TxID, Vout: utxo.Vout}}
	claimAmount := 0.01
	fee := 0.0002
	change := utxo.Amount - claimAmount - fee

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
	script, err := getClaimPayoutScript(claimName, claimBytes, changeAddress)
	checkErr(err)
	tx.AddTxOut(wire.NewTxOut(int64(amount), script))

	// sign and send
	signedTx, allInputsSigned, err := client.SignRawTransaction(tx)
	checkErr(err)
	if !allInputsSigned {
		panic("Not all inputs for the tx could be signed!")
	}

	j, err := json.MarshalIndent(claim, "", "  ")
	checkErr(err)
	fmt.Println(string(j))

	buf := bytes.NewBuffer(make([]byte, 0, signedTx.SerializeSize()))
	if err := tx.Serialize(buf); err != nil {
		panic(err)
	}
	txHex := hex.EncodeToString(buf.Bytes())
	spew.Dump(txHex)
	decoded, err := client.DecodeRawTransaction(buf.Bytes())
	checkErr(err)
	spew.Dump(decoded)

	// upload blobs to reflector
	c := reflector.Client{}
	err = c.Connect("reflector.lbry.com:5566")
	checkErr(err)
	for i, b := range st {
		if i == 0 {
			err = c.SendSDBlob(b)
		} else {
			err = c.SendBlob(b)
		}
		if err != nil {
			panic(err)
		}
	}

	txid, err := client.SendRawTransaction(signedTx, false)
	checkErr(err)
	fmt.Println(txid.String())
}

func claimAndStreamForFile(cmd *cobra.Command, path string) (*pb.Claim, stream.Stream, error) {
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
		Author:      mustGetFlagString(cmd, "author"),
		ReleaseTime: mustGetFlagInt64(cmd, "release-time"),
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
		streamPB.Type = streamVideoMetadata(path)
	case "audio":
		streamPB.Type = &pb.Stream_Audio{}
	case "image":
		streamPB.Type = &pb.Stream_Image{}
	}

	claim := &pb.Claim{
		Title:       mustGetFlagString(cmd, "title"),
		Description: mustGetFlagString(cmd, "description"),
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

func guessMimeType(ext string) (string, string) {
	if ext == "" {
		return "application/octet-stream", "binary"
	}

	ext = strings.ToLower(strings.TrimLeft(strings.TrimSpace(ext), "."))

	types := map[string]struct{ mime, t string }{
		"a":        {"application/octet-stream", "binary"},
		"ai":       {"application/postscript", "image"},
		"aif":      {"audio/x-aiff", "audio"},
		"aifc":     {"audio/x-aiff", "audio"},
		"aiff":     {"audio/x-aiff", "audio"},
		"au":       {"audio/basic", "audio"},
		"avi":      {"video/x-msvideo", "video"},
		"bat":      {"text/plain", "document"},
		"bcpio":    {"application/x-bcpio", "binary"},
		"bin":      {"application/octet-stream", "binary"},
		"bmp":      {"image/bmp", "image"},
		"c":        {"text/plain", "document"},
		"cdf":      {"application/x-netcdf", "binary"},
		"cpio":     {"application/x-cpio", "binary"},
		"csh":      {"application/x-csh", "binary"},
		"css":      {"text/css", "document"},
		"csv":      {"text/csv", "document"},
		"dll":      {"application/octet-stream", "binary"},
		"doc":      {"application/msword", "document"},
		"dot":      {"application/msword", "document"},
		"dvi":      {"application/x-dvi", "binary"},
		"eml":      {"message/rfc822", "document"},
		"eps":      {"application/postscript", "document"},
		"epub":     {"application/epub+zip", "document"},
		"etx":      {"text/x-setext", "document"},
		"exe":      {"application/octet-stream", "binary"},
		"gif":      {"image/gif", "image"},
		"gtar":     {"application/x-gtar", "binary"},
		"h":        {"text/plain", "document"},
		"hdf":      {"application/x-hdf", "binary"},
		"htm":      {"text/html", "document"},
		"html":     {"text/html", "document"},
		"ico":      {"image/vnd.microsoft.icon", "image"},
		"ief":      {"image/ief", "image"},
		"iges":     {"model/iges", "model"},
		"jpe":      {"image/jpeg", "image"},
		"jpeg":     {"image/jpeg", "image"},
		"jpg":      {"image/jpeg", "image"},
		"js":       {"application/javascript", "document"},
		"json":     {"application/json", "document"},
		"ksh":      {"text/plain", "document"},
		"latex":    {"application/x-latex", "binary"},
		"m1v":      {"video/mpeg", "video"},
		"m3u":      {"application/vnd.apple.mpegurl", "audio"},
		"m3u8":     {"application/vnd.apple.mpegurl", "audio"},
		"man":      {"application/x-troff-man", "document"},
		"markdown": {"text/markdown", "document"},
		"md":       {"text/markdown", "document"},
		"me":       {"application/x-troff-me", "binary"},
		"mht":      {"message/rfc822", "document"},
		"mhtml":    {"message/rfc822", "document"},
		"mif":      {"application/x-mif", "binary"},
		"mov":      {"video/quicktime", "video"},
		"movie":    {"video/x-sgi-movie", "video"},
		"mp2":      {"audio/mpeg", "audio"},
		"mp3":      {"audio/mpeg", "audio"},
		"mp4":      {"video/mp4", "video"},
		"mpa":      {"video/mpeg", "video"},
		"mpe":      {"video/mpeg", "video"},
		"mpeg":     {"video/mpeg", "video"},
		"mpg":      {"video/mpeg", "video"},
		"ms":       {"application/x-troff-ms", "binary"},
		"nc":       {"application/x-netcdf", "binary"},
		"nws":      {"message/rfc822", "document"},
		"o":        {"application/octet-stream", "binary"},
		"obj":      {"application/octet-stream", "model"},
		"oda":      {"application/oda", "binary"},
		"p12":      {"application/x-pkcs12", "binary"},
		"p7c":      {"application/pkcs7-mime", "binary"},
		"pbm":      {"image/x-portable-bitmap", "image"},
		"pdf":      {"application/pdf", "document"},
		"pfx":      {"application/x-pkcs12", "binary"},
		"pgm":      {"image/x-portable-graymap", "image"},
		"pl":       {"text/plain", "document"},
		"png":      {"image/png", "image"},
		"pnm":      {"image/x-portable-anymap", "image"},
		"pot":      {"application/vnd.ms-powerpoint", "document"},
		"ppa":      {"application/vnd.ms-powerpoint", "document"},
		"ppm":      {"image/x-portable-pixmap", "image"},
		"pps":      {"application/vnd.ms-powerpoint", "document"},
		"ppt":      {"application/vnd.ms-powerpoint", "document"},
		"ps":       {"application/postscript", "document"},
		"pwz":      {"application/vnd.ms-powerpoint", "document"},
		"py":       {"text/x-python", "document"},
		"pyc":      {"application/x-python-code", "binary"},
		"pyo":      {"application/x-python-code", "binary"},
		"qt":       {"video/quicktime", "video"},
		"ra":       {"audio/x-pn-realaudio", "audio"},
		"ram":      {"application/x-pn-realaudio", "audio"},
		"ras":      {"image/x-cmu-raster", "image"},
		"rdf":      {"application/xml", "binary"},
		"rgb":      {"image/x-rgb", "image"},
		"roff":     {"application/x-troff", "binary"},
		"rtx":      {"text/richtext", "document"},
		"sgm":      {"text/x-sgml", "document"},
		"sgml":     {"text/x-sgml", "document"},
		"sh":       {"application/x-sh", "document"},
		"shar":     {"application/x-shar", "binary"},
		"snd":      {"audio/basic", "audio"},
		"so":       {"application/octet-stream", "binary"},
		"src":      {"application/x-wais-source", "binary"},
		"stl":      {"model/stl", "model"},
		"sv4cpio":  {"application/x-sv4cpio", "binary"},
		"sv4crc":   {"application/x-sv4crc", "binary"},
		"svg":      {"image/svg+xml", "image"},
		"swf":      {"application/x-shockwave-flash", "binary"},
		"t":        {"application/x-troff", "binary"},
		"tar":      {"application/x-tar", "binary"},
		"tcl":      {"application/x-tcl", "binary"},
		"tex":      {"application/x-tex", "binary"},
		"texi":     {"application/x-texinfo", "binary"},
		"texinfo":  {"application/x-texinfo", "binary"},
		"tif":      {"image/tiff", "image"},
		"tiff":     {"image/tiff", "image"},
		"tr":       {"application/x-troff", "binary"},
		"tsv":      {"text/tab-separated-values", "document"},
		"txt":      {"text/plain", "document"},
		"ustar":    {"application/x-ustar", "binary"},
		"vcf":      {"text/x-vcard", "document"},
		"wav":      {"audio/x-wav", "audio"},
		"webm":     {"video/webm", "video"},
		"wiz":      {"application/msword", "document"},
		"wsdl":     {"application/xml", "document"},
		"xbm":      {"image/x-xbitmap", "image"},
		"xlb":      {"application/vnd.ms-excel", "document"},
		"xls":      {"application/vnd.ms-excel", "document"},
		"xml":      {"text/xml", "document"},
		"xpdl":     {"application/xml", "document"},
		"xpm":      {"image/x-xpixmap", "image"},
		"xsl":      {"application/xml", "document"},
		"xwd":      {"image/x-xwindowdump", "image"},
		"zip":      {"application/zip", "binary"},

		// These are non-standard types, commonly found in the wild.
		"cbr":  {"application/vnd.comicbook-rar", "document"},
		"cbz":  {"application/vnd.comicbook+zip", "document"},
		"flac": {"audio/flac", "audio"},
		"lbry": {"application/x-ext-lbry", "document"},
		"m4v":  {"video/m4v", "video"},
		"mid":  {"audio/midi", "audio"},
		"midi": {"audio/midi", "audio"},
		"mkv":  {"video/x-matroska", "video"},
		"mobi": {"application/x-mobipocket-ebook", "document"},
		"oga":  {"audio/ogg", "audio"},
		"ogv":  {"video/ogg", "video"},
		"pct":  {"image/pict", "image"},
		"pic":  {"image/pict", "image"},
		"pict": {"image/pict", "image"},
		"prc":  {"application/x-mobipocket-ebook", "document"},
		"rtf":  {"application/rtf", "document"},
		"xul":  {"text/xul", "document"},

		// microsoft is special and has its own "standard"
		// https://docs.microsoft.com/en-us/windows/desktop/wmp/file-name-extensions
		"wmv": {"video/x-ms-wmv", "video"},
	}

	if data, ok := types[ext]; ok {
		return data.mime, data.t
	}
	return "application/x-ext-" + ext, "binary"
}

func streamVideoMetadata(path string) *pb.Stream_Video {
	mi, err := mediainfo.GetMediaInfo(path)
	checkErr(err)
	return &pb.Stream_Video{
		Video: &pb.Video{
			Duration: uint32(mi.General.Duration / 1000),
			Height:   uint32(mi.Video.Height),
			Width:    uint32(mi.Video.Width),
		},
	}
}
