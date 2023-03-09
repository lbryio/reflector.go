package cmd

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/lbryio/reflector.go/reflector"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/spf13/cobra"
)

func init() {
	var cmd = &cobra.Command{
		Use:   "send ADDRESS:PORT PATH",
		Short: "Send a file to a reflector",
		Args:  cobra.ExactArgs(2),
		Run:   sendCmd,
	}
	cmd.PersistentFlags().String("sd-cache", "", "path to dir where sd blobs will be cached")
	rootCmd.AddCommand(cmd)
}

// todo: if retrying a large file is slow, we can add the ability to seek ahead in the file so we're not
// re-uploading blobs that already exist

var hackyReflector reflector.Client

func sendCmd(cmd *cobra.Command, args []string) {
	reflectorAddress := args[0]
	err := hackyReflector.Connect(reflectorAddress)
	checkErr(err)
	defer func() { _ = hackyReflector.Close() }()

	filePath := args[1]
	file, err := os.Open(filePath)
	checkErr(err)
	defer func() { _ = file.Close() }()
	sdCachePath := ""
	sdCacheDir := mustGetFlagString(cmd, "sd-cache")
	if sdCacheDir != "" {
		if _, err := os.Stat(sdCacheDir); os.IsNotExist(err) {
			err = os.MkdirAll(sdCacheDir, 0777)
			checkErr(err)
		}
		sdCachePath = path.Join(sdCacheDir, filePath+".sdblob")
	}

	var enc *stream.Encoder

	if sdCachePath != "" {
		if _, err := os.Stat(sdCachePath); !os.IsNotExist(err) {
			sdBlob, err := os.ReadFile(sdCachePath)
			checkErr(err)
			cachedSDBlob := &stream.SDBlob{}
			err = cachedSDBlob.FromBlob(sdBlob)
			checkErr(err)
			enc = stream.NewEncoderFromSD(file, cachedSDBlob)
		}
	}
	if enc == nil {
		enc = stream.NewEncoder(file)
	}

	exitCode := 0

	var killed bool
	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-interruptChan
		fmt.Printf("caught %s, exiting...\n", sig.String())
		killed = true
		exitCode = 1
	}()

	for {
		if killed {
			break
		}

		b, err := enc.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fmt.Printf("error reading next blob: %v\n", err)
			exitCode = 1
			break
		}

		err = hackyReflect(b, false)
		if err != nil {
			fmt.Printf("error reflecting blob %s: %v\n", b.HashHex()[:8], err)
			exitCode = 1
			break
		}
	}

	sd := enc.SDBlob()
	//sd.StreamName = filepath.Base(filePath)
	//sd.SuggestedFileName = filepath.Base(filePath)
	err = os.WriteFile(sdCachePath, sd.ToBlob(), 0666)
	if err != nil {
		fmt.Printf("error saving sd blob: %v\n", err)
		fmt.Println(sd.ToJson())
		exitCode = 1
	}

	if killed {
		os.Exit(exitCode)
	}

	if reflectorAddress != "" {
		err = hackyReflect(sd.ToBlob(), true)
		if err != nil {
			fmt.Printf("error reflecting sd blob %s: %v\n", sd.HashHex()[:8], err)
			exitCode = 1
		}
	}

	ret := struct {
		SDHash     string `json:"sd_hash"`
		SourceHash string `json:"source_hash"`
	}{
		SDHash:     sd.HashHex(),
		SourceHash: hex.EncodeToString(enc.SourceHash()),
	}

	j, err := json.MarshalIndent(ret, "", "  ")
	checkErr(err)
	fmt.Println(string(j))
	os.Exit(exitCode)
}

func hackyReflect(b stream.Blob, sd bool) error {
	var err error
	if sd {
		err = hackyReflector.SendSDBlob(b)
	} else {
		err = hackyReflector.SendBlob(b)
	}

	if errors.Is(err, reflector.ErrBlobExists) {
		//fmt.Printf("%s already reflected\n", b.HashHex()[:8])
		return nil
	}

	return err
}
