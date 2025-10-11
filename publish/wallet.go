package publish

import (
	"encoding/json"
	"io"
)

func LoadWallet(r io.Reader) (WalletFile, error) {
	var w WalletFile
	err := json.NewDecoder(r).Decode(&w)
	return w, err
}

type WalletFile struct {
	Name        string      `json:"name"`
	Accounts    []Account   `json:"accounts"`
	Preferences WalletPrefs `json:"preferences"`
	Version     int         `json:"version"`
}

type Account struct {
	Certificates     map[string]string `json:"certificates"`
	Ledger           string            `json:"ledger"`
	Name             string            `json:"name"`
	PrivateKey       string            `json:"private_key"`
	PublicKey        string            `json:"public_key"`
	Seed             string            `json:"seed"`
	AddressGenerator AddressGenerator  `json:"address_generator"`
	ModifiedOn       float64           `json:"modified_on"`
	Encrypted        bool              `json:"encrypted"`
}

type AddressGenerator struct {
	Name      string           `json:"name"`
	Change    AddressGenParams `json:"change"` // should "change" and "receiving" be replaced with a map[string]AddressGenParams?
	Receiving AddressGenParams `json:"receiving"`
}

type AddressGenParams struct {
	Gap                   int `json:"gap"`
	MaximumUsesPerAddress int `json:"maximum_uses_per_address"`
}

type WalletPrefs struct {
	Shared struct {
		Value struct {
			Type    string `json:"type"`
			Version string `json:"version"`
			Value   struct {
				Blocked           []interface{} `json:"blocked"`
				Subscriptions     []string      `json:"subscriptions"`
				Tags              []string      `json:"tags"`
				AppWelcomeVersion int           `json:"app_welcome_version"`
				Sharing3P         bool          `json:"sharing_3P"`
			} `json:"value"`
		} `json:"value"`
		Ts float64 `json:"ts"`
	} `json:"shared"`
}
