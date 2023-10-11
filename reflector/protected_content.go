package reflector

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/bluele/gcache"
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"golang.org/x/sync/singleflight"
)

const protectedListURL = "https://api.odysee.com/file/list_protected"

type ProtectedContent struct {
	SDHash  string `json:"sd_hash"`
	ClaimID string `json:"claim_id"`
}

var protectedCache = gcache.New(10).Expiration(2 * time.Minute).Build()

func GetProtectedContent() (map[string]bool, error) {
	cachedVal, err := protectedCache.Get("protected")
	if err == nil && cachedVal != nil {
		return cachedVal.(map[string]bool), nil
	}

	method := "GET"
	var r struct {
		Success bool               `json:"success"`
		Error   string             `json:"error"`
		Data    []ProtectedContent `json:"data"`
	}

	client := &http.Client{}
	req, err := http.NewRequest(method, protectedListURL, nil)

	if err != nil {
		return nil, errors.Err(err)
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return nil, errors.Err("unexpected status code %d", res.StatusCode)
	}
	if err = json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, errors.Err(err)
	}

	if !r.Success {
		return nil, errors.Prefix("file/list_protected API call", r.Error)
	}

	protectedMap := make(map[string]bool, len(r.Data))
	for _, pc := range r.Data {
		protectedMap[pc.SDHash] = true
	}
	err = protectedCache.Set("protected", protectedMap)
	if err != nil {
		return protectedMap, errors.Err(err)
	}
	return protectedMap, nil
}

var sf = singleflight.Group{}

func IsProtected(sdHash string) bool {
	val, err, _ := sf.Do("protected", func() (interface{}, error) {
		protectedMap, err := GetProtectedContent()
		if err != nil {
			return nil, err
		}
		return protectedMap[sdHash], nil
	})
	if err != nil {
		return false
	}
	return val.(bool)
}
