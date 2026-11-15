module use-riblt

go 1.27.1

require (
	github.com/golang/protobuf v1.5.4
	github.com/joonnna/ifrit v0.0.0
	github.com/yangl1996/riblt v0.1.2
	google.golang.org/grpc v1.85.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/dchest/siphash v1.2.3 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261110000943-09e21b237f2c // indirect
)

replace github.com/joonnna/ifrit => github.com/eika123/ifrit v0.0.0-20261112215735-8695069d3f06
