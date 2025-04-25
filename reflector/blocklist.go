package reflector

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/store"
	"github.com/lbryio/reflector.go/wallet"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/stop"

	log "github.com/sirupsen/logrus"
)

const blocklistURL = "https://api.lbry.com/file/list_blocked"

func (s *Server) enableBlocklist(b store.Blocklister) {
	walletServers := []string{
		"a-hub1.odysee.com:50001",
		"b-hub1.odysee.com:50001",
		"c-hub1.odysee.com:50001",
		"s-hub1.odysee.com:50001",
	}

	updateBlocklist(b, walletServers, s.grp.Ch())
	t := time.NewTicker(12 * time.Hour)
	for {
		select {
		case <-s.grp.Ch():
			return
		case <-t.C:
			updateBlocklist(b, walletServers, s.grp.Ch())
		}
	}
}

func updateBlocklist(b store.Blocklister, walletServers []string, stopper stop.Chan) {
	log.Debugf("blocklist update starting")
	values, err := blockedSdHashes(walletServers, stopper)
	if err != nil {
		log.Error(err)
		return
	}

	for name, v := range values {
		if v.Err != nil {
			log.Error(errors.FullTrace(errors.Err("blocklist: %s: %s", name, v.Err)))
			continue
		}

		err = b.Block(v.Value)
		if err != nil {
			log.Error(err)
		}
	}
	log.Debugf("blocklist update done")
}

func blockedSdHashes(walletServers []string, stopper stop.Chan) (map[string]valOrErr, error) {
	client := http.Client{Timeout: 1 * time.Second}
	resp, err := client.Get(blocklistURL)
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

	return sdHashesForOutpoints(walletServers, r.Data.Outpoints, stopper)
}

type valOrErr struct {
	Value string
	Err   error
}

// sdHashesForOutpoints queries wallet server for the sd hashes in a given outpoints
func sdHashesForOutpoints(walletServers, outpoints []string, stopper stop.Chan) (map[string]valOrErr, error) {
	values := make(map[string]valOrErr)

	node := wallet.NewNode()
	err := node.Connect(walletServers, nil)
	if err != nil {
		return nil, errors.Err(err)
	}

	done := make(chan bool)
	metrics.RoutinesQueue.WithLabelValues("reflector", "sdhashesforoutput").Inc()
	go func() {
		defer metrics.RoutinesQueue.WithLabelValues("reflector", "sdhashesforoutput").Dec()
		select {
		case <-done:
		case <-stopper:
		}
		node.Shutdown()
	}()

OutpointLoop:
	for _, outpoint := range outpoints {
		select {
		case <-stopper:
			break OutpointLoop
		default:
		}

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

	select {
	case done <- true:
	default: // in case of race where stopper got stopped right after loop finished
	}

	return values, nil
}
