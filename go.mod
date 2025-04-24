module github.com/lbryio/reflector.go

go 1.23.0

toolchain go1.23.6

replace github.com/btcsuite/btcd => github.com/lbryio/lbrycrd.go v0.0.0-20200203050410-e1076f12bf19

require (
	github.com/aws/aws-sdk-go v1.55.6
	github.com/bluele/gcache v0.0.2
	github.com/brk0v/directio v0.0.0-20241105172640-ae9d82eb8fee
	github.com/btcsuite/btcd v0.24.2
	github.com/btcsuite/btcutil v1.0.2
	github.com/c2h5oh/datasize v0.0.0-20231215233829-aa82cc1e6500
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc
	github.com/ekyoung/gin-nice-recovery v0.0.0-20160510022553-1654dca486db
	github.com/gin-gonic/gin v1.10.0
	github.com/go-sql-driver/mysql v1.8.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.4
	github.com/google/gops v0.3.28
	github.com/gorilla/mux v1.8.1
	github.com/hashicorp/serf v0.10.2
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf
	github.com/johntdyer/slackrus v0.0.0-20230315191314-80bc92dee4fc
	github.com/karrick/godirwalk v1.17.0
	github.com/lbryio/chainquery v1.9.1-0.20240927170248-48c092515dea
	github.com/lbryio/lbry.go/v2 v2.7.2-0.20230307181431-a01aa6dc0629
	github.com/lbryio/types v0.0.0-20220224142228-73610f6654a6
	github.com/phayes/freeport v0.0.0-20220201140144-74d24b5ae9f5
	github.com/prometheus/client_golang v1.20.5
	github.com/quic-go/quic-go v0.49.0
	github.com/sirupsen/logrus v1.9.3
	github.com/spf13/cast v1.7.1
	github.com/spf13/cobra v1.8.1
	github.com/spf13/viper v1.19.0
	github.com/stretchr/testify v1.10.0
	github.com/volatiletech/null/v8 v8.1.2
	go.uber.org/atomic v1.11.0
	golang.org/x/sync v0.11.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/btcsuite/btclog v1.0.0 // indirect
	github.com/btcsuite/go-socks v0.0.0-20170105172521-4720035b7bfd // indirect
	github.com/btcsuite/websocket v0.0.0-20150119174127-31079b680792 // indirect
	github.com/bytedance/sonic v1.12.8 // indirect
	github.com/bytedance/sonic/loader v0.2.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudwego/base64x v0.1.5 // indirect
	github.com/friendsofgo/errors v0.9.2 // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.8 // indirect
	github.com/gin-contrib/sse v1.0.0 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.24.0 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/pprof v0.0.0-20250208200701-d0013a598941 // indirect
	github.com/gorilla/rpc v1.2.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-metrics v0.5.4 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.5 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/hashicorp/memberlist v0.5.2 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/johntdyer/slack-go v0.0.0-20230314151037-c5bf334f9b6e // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/lyoshenka/bencode v0.0.0-20180323155644-b7abd7672df5 // indirect
	github.com/magiconair/properties v1.8.9 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/miekg/dns v1.1.56 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/onsi/ginkgo/v2 v2.22.2 // indirect
	github.com/pelletier/go-toml/v2 v2.2.3 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/quic-go/qpack v0.5.1 // indirect
	github.com/sagikazarmark/locafero v0.7.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/slack-go/slack v0.16.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/afero v1.12.0 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.2.12 // indirect
	github.com/volatiletech/inflect v0.0.1 // indirect
	github.com/volatiletech/randomize v0.0.1 // indirect
	github.com/volatiletech/strmangle v0.0.8 // indirect
	go.uber.org/mock v0.5.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/arch v0.14.0 // indirect
	golang.org/x/crypto v0.33.0 // indirect
	golang.org/x/exp v0.0.0-20250210185358-939b2ce775ac // indirect
	golang.org/x/mod v0.23.0 // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	golang.org/x/time v0.8.0 // indirect
	golang.org/x/tools v0.30.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/protobuf v1.36.5 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/nullbio/null.v6 v6.0.0-20161116030900-40264a2e6b79 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
