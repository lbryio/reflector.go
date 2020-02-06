package reflector

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lbryio/reflector.go/store"
	"github.com/lbryio/reflector.go/wallet"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	log "github.com/sirupsen/logrus"
)

const blocklistURL = "https://api.lbry.com/file/list_blocked"

func (s *Server) enableBlocklist(b store.Blocklister) {
	// TODO: updateBlocklist should be killed when server is shutting down
	updateBlocklist(b)
	t := time.NewTicker(12 * time.Hour)
	for {
		select {
		case <-s.grp.Ch():
			return
		case <-t.C:
			updateBlocklist(b)
		}
	}
}

func updateBlocklist(b store.Blocklister) {
	values, err := blockedSdHashes()
	if err != nil {
		log.Error(err)
		return
	}

	for name, v := range values {
		if v.Err != nil {
			log.Error(errors.Err("blocklist: %s: %s", name, v.Err))
			continue
		}

		err = b.Block(v.Value)
		if err != nil {
			log.Error(err)
		}
	}
}

func blockedSdHashes() (map[string]valOrErr, error) {
	resp, err := http.Get(blocklistURL)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			log.Errorln(errors.Err(err))
		}
	}()

	var r struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
		Data    struct {
			Outpoints []string `json:"outpoints"`
		} `json:"data"`
	}

	if err = json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, errors.Err(err)
	}

	if !r.Success {
		return nil, errors.Prefix("list_blocked API call", r.Error)
	}

	return sdHashesForOutpoints(r.Data.Outpoints)
}

type valOrErr struct {
	Value string
	Err   error
}

// sdHashesForOutpoints queries wallet server for the sd hashes in a given outpoints
func sdHashesForOutpoints(outpoints []string) (map[string]valOrErr, error) {
	values := make(map[string]valOrErr)

	node := wallet.NewNode()
	defer node.Shutdown()
	err := node.Connect([]string{
		"spv4.lbry.com:50001",
		"spv5.lbry.com:50001",
		"spv7.lbry.com:50001",
		"spv9.lbry.com:50001",
		"spv15.lbry.com:50001",
		"spv17.lbry.com:50001",
		"spv19.1bry.com:50001",
		"spv25.lbry.com:50001",
		"spv26.lbry.com:50001",
	}, nil)
	if err != nil {
		return nil, errors.Err(err)
	}

	for _, outpoint := range outpoints {
		parts := strings.Split(outpoint, ":")
		if len(parts) != 2 {
			values[outpoint] = valOrErr{Err: errors.Err("invalid outpoint format")}
			continue
		}

		nout, err := strconv.Atoi(parts[1])
		if err != nil {
			values[outpoint] = valOrErr{Err: errors.Prefix("invalid nout", err)}
			continue
		}

		claim, err := node.GetClaimInTx(parts[0], nout)
		if err != nil {
			values[outpoint] = valOrErr{Err: err}
			continue
		}

		hash := hex.EncodeToString(claim.GetStream().GetSource().GetSdHash())
		values[outpoint] = valOrErr{Value: hash, Err: nil}
	}

	return values, nil
}
