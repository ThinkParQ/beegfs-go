module github.com/thinkparq/beegfs-ctl

go 1.21.1

require (
	github.com/spf13/cobra v1.7.0
	github.com/thinkparq/protobuf v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.57.0
)

replace github.com/thinkparq/protobuf => /data/git/protobuf

require (
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/net v0.14.0 // indirect
	golang.org/x/sys v0.11.0 // indirect
	golang.org/x/text v0.12.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230807174057-1744710a1577 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)
