package reflector

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/bluele/gcache"
	"golang.org/x/sync/singleflight"
)

const protectedListURL = "https://direct.api.odysee.com/file/list_protected"

type ProtectedContent struct {
	SDHash  string `json:"sd_hash"`
	ClaimID string `json:"claim_id"`
}

var protectedCache = gcache.New(10).Expiration(2 * time.Minute).Build()

func GetProtectedContent() (interface{}, error) {
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

	// Bound the request to avoid hanging the entire request path.
	// Without a timeout, a slow or unreachable endpoint can block
	// singleflight callers indefinitely and stall HTTP handlers.
	client := &http.Client{Timeout: 5 * time.Second}
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
	val, err, _ := sf.Do("protected", GetProtectedContent)
	if err != nil {
		return false
	}
	cachedMap, ok := val.(map[string]bool)
	if !ok {
		return false
	}

	return cachedMap[sdHash]
}
