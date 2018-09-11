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

func BlockedSdHashes() (map[string]ValueResp, error) {
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

	return sdHashesForOutpoints(r.Data.Outpoints)
}

type ValueResp struct {
	Value string
	Err   error
}

// sdHashForOutpoint queries wallet server for the sd hash in a given outpoint
func sdHashForOutpoint(outpoint string) (string, error) {
	vals, err := sdHashesForOutpoints([]string{outpoint})
	if err != nil {
		return "", err
	}

	val, ok := vals[outpoint]
	if !ok {
		return "", errors.Err("outpoint not in response")
	}

	return val.Value, val.Err
}

// sdHashesForOutpoints queries wallet server for the sd hashes in a given outpoints
func sdHashesForOutpoints(outpoints []string) (map[string]ValueResp, error) {
	values := make(map[string]ValueResp)

	node := wallet.NewNode()
	err := node.ConnectTCP("victor.lbry.tech:50001")
	if err != nil {
		return nil, err
	}

	for _, outpoint := range outpoints {
		parts := strings.Split(outpoint, ":")
		if len(parts) != 2 {
			values[outpoint] = ValueResp{Err: errors.Err("invalid outpoint format")}
			continue
		}

		nout, err := strconv.Atoi(parts[1])
		if err != nil {
			values[outpoint] = ValueResp{Err: errors.Prefix("invalid nout", err)}
			continue
		}

		resp, err := node.GetClaimsInTx(parts[0])
		if err != nil {
			values[outpoint] = ValueResp{Err: err}
			continue
		}

		var value []byte
		for _, tx := range resp.Result {
			if tx.Nout != nout {
				continue
			}

			value, err = hex.DecodeString(tx.Value)
			break
		}
		if err != nil {
			values[outpoint] = ValueResp{Err: err}
			continue
		}

		claim := &types.Claim{}
		err = proto.Unmarshal(value, claim)
		if err != nil {
			values[outpoint] = ValueResp{Err: err}
			continue
		}

		if claim.GetStream().GetSource().GetSourceType() != types.Source_lbry_sd_hash {
			values[outpoint] = ValueResp{Err: errors.Err("source is nil or source type is not lbry_sd_hash")}
			continue
		}

		values[outpoint] = ValueResp{Value: hex.EncodeToString(claim.GetStream().GetSource().GetSource())}
	}

	return values, nil
}
