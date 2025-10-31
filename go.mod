module github.com/thinkparq/beegfs-go

go 1.25.3

tool (
	github.com/google/go-licenses
	golang.org/x/vuln/cmd/govulncheck
	honnef.co/go/tools/cmd/staticcheck
)

require (
	github.com/aws/aws-sdk-go-v2 v1.36.4
	github.com/aws/aws-sdk-go-v2/config v1.29.16
	github.com/aws/aws-sdk-go-v2/credentials v1.17.69
	github.com/aws/aws-sdk-go-v2/service/s3 v1.80.2
	github.com/aws/smithy-go v1.22.3
	github.com/bmatcuk/doublestar/v4 v4.8.1
	github.com/dgraph-io/badger/v4 v4.7.0
	github.com/dsnet/golib/unitconv v1.0.2
	github.com/go-viper/mapstructure/v2 v2.4.0
	github.com/google/uuid v1.6.0
	github.com/jedib0t/go-pretty/v6 v6.6.7
	github.com/mitchellh/go-wordwrap v1.0.1
	github.com/spf13/afero v1.14.0
	github.com/spf13/cobra v1.9.1
	github.com/spf13/pflag v1.0.6
	github.com/spf13/viper v1.20.1
	github.com/stretchr/testify v1.10.0
	github.com/thinkparq/protobuf v0.8.2-0.20250915082136-5706c6e57695
	go.uber.org/zap v1.27.0
	golang.org/x/sync v0.15.0
	golang.org/x/sys v0.33.0
	golang.org/x/term v0.32.0
	google.golang.org/grpc v1.73.0
	google.golang.org/protobuf v1.36.6
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
)

require (
	github.com/BurntSushi/toml v1.4.1-0.20240526193622-a339e1f7089c // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.10 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.31 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.35 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.7.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.25.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.30.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.21 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgraph-io/ristretto/v2 v2.2.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/emirpasic/gods v1.12.0 // indirect
	github.com/expr-lang/expr v1.17.5
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/go-licenses v1.6.0 // indirect
	github.com/google/licenseclassifier v0.0.0-20210722185704-3043a050f148 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jbenet/go-context v0.0.0-20150711004518-d14ea06fba99 // indirect
	github.com/kevinburke/ssh_config v0.0.0-20190725054713-01f96b0aa0cd // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/otiai10/copy v1.6.0 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/sagikazarmark/locafero v0.9.0 // indirect
	github.com/sergi/go-diff v1.2.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/cast v1.9.2 // indirect
	github.com/src-d/gcfg v1.4.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/xanzy/ssh-agent v0.2.1 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel v1.36.0 // indirect
	go.opentelemetry.io/otel/metric v1.36.0 // indirect
	go.opentelemetry.io/otel/trace v1.36.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.39.0 // indirect
	golang.org/x/exp/typeparams v0.0.0-20231108232855-2478ac86f678 // indirect
	golang.org/x/mod v0.25.0 // indirect
	golang.org/x/net v0.41.0 // indirect
	golang.org/x/telemetry v0.0.0-20240522233618-39ace7a40ae7 // indirect
	golang.org/x/text v0.26.0 // indirect
	golang.org/x/tools v0.33.0 // indirect
	golang.org/x/vuln v1.1.4 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250603155806-513f23925822 // indirect
	gopkg.in/src-d/go-billy.v4 v4.3.2 // indirect
	gopkg.in/src-d/go-git.v4 v4.13.1 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	honnef.co/go/tools v0.6.1 // indirect
	k8s.io/klog/v2 v2.80.1 // indirect
)
