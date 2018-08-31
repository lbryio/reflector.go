package wallet

// ServerVersion returns the server's version.
// https://electrumx.readthedocs.io/en/latest/protocol-methods.html#server-version
func (n *Node) ServerVersion() (string, error) {
	resp := &struct {
		Result []string `json:"result"`
	}{}
	err := n.request("server.version", []string{"reflector.go", ProtocolVersion}, resp)

	var v string
	if len(resp.Result) >= 2 {
		v = resp.Result[1]
	}

	return v, err
}

type GetClaimsInTxResp struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  []struct {
		Name            string        `json:"name"`
		ClaimID         string        `json:"claim_id"`
		Txid            string        `json:"txid"`
		Nout            int           `json:"nout"`
		Amount          int           `json:"amount"`
		Depth           int           `json:"depth"`
		Height          int           `json:"height"`
		Value           string        `json:"value"`
		ClaimSequence   int           `json:"claim_sequence"`
		Address         string        `json:"address"`
		Supports        []interface{} `json:"supports"` // TODO: finish me
		EffectiveAmount int           `json:"effective_amount"`
		ValidAtHeight   int           `json:"valid_at_height"`
	} `json:"result"`
}

func (n *Node) GetClaimsInTx(txid string) (*GetClaimsInTxResp, error) {
	var resp GetClaimsInTxResp
	err := n.request("blockchain.claimtrie.getclaimsintx", []string{txid}, &resp)
	return &resp, err
}
