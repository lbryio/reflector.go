package reflector

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/lbryio/reflector.go/wallet"

	"github.com/lbryio/lbry.go/errors"
	types "github.com/lbryio/types/go"

	"github.com/golang/protobuf/proto"
)

const blocklistURL = "https://api.lbry.io/file/list_blocked"

type blockListResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
	Data    struct {
		Outpoints []string `json:"outpoints"`
	} `json:"data"`
}

func BlockedSdHashes() (map[string]string, error) {
	blocked := make(map[string]string)

	resp, err := http.Get(blocklistURL)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer resp.Body.Close()

	var r blockListResponse
	if err = json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, errors.Err(err)
	}

	if !r.Success {
		return nil, errors.Prefix("list_blocked API call", r.Error)
	}

	for _, outpoint := range r.Data.Outpoints {
		sdHash, err := sdHashForOutpoint(outpoint)
		if err != nil {
			blocked[outpoint] = err.Error()
		} else {
			blocked[outpoint] = sdHash
		}
	}

	return blocked, nil
}

func sdHashForOutpoint(outpoint string) (string, error) {
	val, err := valueForOutpoint(outpoint)
	if err != nil {
		return "", err
	}

	return sdHashForValue(val)
}

// decodeValue decodes a protobuf-encoded claim and returns the sd hash
func sdHashForValue(value []byte) (string, error) {
	claim := &types.Claim{}
	err := proto.Unmarshal(value, claim)
	if err != nil {
		return "", errors.Err(err)
	}

	if claim.GetStream().GetSource().GetSourceType() != types.Source_lbry_sd_hash {
		return "", errors.Err("source is nil or source type is not lbry_sd_hash")
	}

	return hex.EncodeToString(claim.GetStream().GetSource().GetSource()), nil
}

// valueForOutpoint queries wallet server for the value of the claim at the given outpoint
func valueForOutpoint(outpoint string) ([]byte, error) {
	parts := strings.Split(outpoint, ":")
	if len(parts) != 2 {
		return nil, errors.Err("invalid outpoint format")
	}

	nout, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, errors.Prefix("invalid nout", err)
	}

	node := wallet.NewNode()
	err = node.ConnectTCP("victor.lbry.tech:50001")
	if err != nil {
		return nil, err
	}

	resp, err := node.GetClaimsInTx(parts[0])
	if err != nil {
		return nil, err
	}

	for _, tx := range resp.Result {
		if tx.Nout == nout {
			return hex.DecodeString(tx.Value)
		}
	}

	return nil, errors.Err("outpoint not found")
}
