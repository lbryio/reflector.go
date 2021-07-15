module github.com/lbryio/reflector.go

replace github.com/btcsuite/btcd => github.com/lbryio/lbrycrd.go v0.0.0-20200203050410-e1076f12bf19

//replace github.com/lbryio/lbry.go/v2 => ../lbry.go

require (
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878 // indirect
	github.com/aws/aws-sdk-go v1.27.0
	github.com/bluele/gcache v0.0.2
	github.com/bparli/lfuda-go v0.3.1
	github.com/brk0v/directio v0.0.0-20190225130936-69406e757cf7
	github.com/btcsuite/btcd v0.0.0-20190824003749-130ea5bddde3
	github.com/btcsuite/btcutil v0.0.0-20190425235716-9e5f4b9a998d
	github.com/c2h5oh/datasize v0.0.0-20200825124411-48ed595a09d2
	github.com/davecgh/go-spew v1.1.1
	github.com/ekyoung/gin-nice-recovery v0.0.0-20160510022553-1654dca486db
	github.com/gin-gonic/gin v1.7.1
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp v0.5.6 // indirect
	github.com/google/gops v0.3.18
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/serf v0.9.5
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf
	github.com/johntdyer/slackrus v0.0.0-20210521205746-42486fb4c48c
	github.com/karrick/godirwalk v1.16.1
	github.com/lbryio/chainquery v1.9.0
	github.com/lbryio/lbry.go/v2 v2.7.2-0.20210416195322-6516df1418e3
	github.com/lbryio/types v0.0.0-20201019032447-f0b4476ef386
	github.com/lucas-clemente/quic-go v0.20.1
	github.com/phayes/freeport v0.0.0-20171002185219-e27662a4a9d6
	github.com/prometheus/client_golang v1.10.0
	github.com/sergi/go-diff v1.2.0 // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/afero v1.4.1 // indirect
	github.com/spf13/cast v1.3.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/viper v1.7.1 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/volatiletech/null v8.0.0+incompatible
	go.uber.org/atomic v1.7.0
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
	golang.org/x/text v0.3.6 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
)

go 1.16
