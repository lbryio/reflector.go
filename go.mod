module github.com/lbryio/reflector.go

replace github.com/btcsuite/btcd => github.com/lbryio/lbrycrd.go v0.0.0-20200203050410-e1076f12bf19

require (
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878 // indirect
	github.com/aws/aws-sdk-go v1.16.11
	github.com/bparli/lfuda-go v0.3.0
	github.com/btcsuite/btcd v0.0.0-20190824003749-130ea5bddde3
	github.com/btcsuite/btcutil v0.0.0-20190425235716-9e5f4b9a998d
	github.com/davecgh/go-spew v1.1.1
	github.com/go-sql-driver/mysql v1.4.1
	github.com/golang/protobuf v1.4.2
	github.com/google/btree v1.0.0 // indirect
	github.com/google/gops v0.3.7
	github.com/google/martian v2.1.0+incompatible
	github.com/gorilla/mux v1.7.4
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/golang-lru v0.5.4
	github.com/hashicorp/memberlist v0.1.4 // indirect
	github.com/hashicorp/serf v0.8.2
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf
	github.com/johntdyer/slackrus v0.0.0-20180518184837-f7aae3243a07
	github.com/karrick/godirwalk v1.16.1
	github.com/lbryio/chainquery v1.9.0
	github.com/lbryio/lbry.go v1.1.2 // indirect
	github.com/lbryio/lbry.go/v2 v2.6.1-0.20200901175808-73382bb02128
	github.com/lbryio/types v0.0.0-20191228214437-05a22073b4ec
	github.com/lucas-clemente/quic-go v0.19.2
	github.com/phayes/freeport v0.0.0-20171002185219-e27662a4a9d6
	github.com/prometheus/client_golang v0.9.2
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/afero v1.4.1 // indirect
	github.com/spf13/cast v1.3.0
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/volatiletech/null v8.0.0+incompatible
	go.uber.org/atomic v1.5.1
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9
	golang.org/x/lint v0.0.0-20191125180803-fdd1cda4f05f // indirect
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/tools v0.0.0-20191227053925-7b8e75db28f4 // indirect
	google.golang.org/appengine v1.6.2 // indirect
)

go 1.15
