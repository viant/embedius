module github.com/viant/embedius

go 1.23.4

toolchain go1.23.7

require (
	github.com/google/uuid v1.6.0
	github.com/minio/highwayhash v1.0.3
	github.com/stretchr/testify v1.10.0
	github.com/viant/afs v1.25.1
	github.com/viant/bintly v0.2.0
	github.com/viant/gds v0.5.0
	github.com/viant/scy v0.15.4
	golang.org/x/sys v0.35.0
	modernc.org/sqlite v1.18.1
)

replace github.com/viant/gds => ../gds

require (
	cloud.google.com/go/compute/metadata v0.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-sqlite3 v1.14.17 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/rogpeppe/go-internal v1.11.0 // indirect
	github.com/viant/toolbox v0.36.0 // indirect
	github.com/viant/vec v0.1.1-0.20240628004145-aad750556278 // indirect
	github.com/viant/xreflect v0.6.2 // indirect
	github.com/viant/xunsafe v0.9.3-0.20240530173106-69808f27713b // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.51.0 // indirect
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/mod v0.23.0 // indirect
	golang.org/x/oauth2 v0.30.0 // indirect
	golang.org/x/sync v0.9.0 // indirect
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d // indirect
	google.golang.org/api v0.183.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240604185151-ef581f913117 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	lukechampine.com/uint128 v1.2.0 // indirect
	modernc.org/cc/v3 v3.36.3 // indirect
	modernc.org/ccgo/v3 v3.16.9 // indirect
	modernc.org/libc v1.17.1 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.2.1 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.1.3 // indirect
	modernc.org/token v1.0.0 // indirect
)
