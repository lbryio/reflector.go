# Reflector

Reflector is a central piece of software that providers LBRY with the following features:
- Blobs reflection: when something is published, we capture the data and store it on our servers for quicker retrieval
- Blobs distribution: when a piece of content is requested and the LBRY network doesn't have it, reflector will retrieve it from its storage and distribute it
- Blobs caching: reflectors can be chained together in multiple regions or servers to form a chain of cached content. We call those "blobcaches". They are layered so that content distribution is favorable in all the regions we deploy it to

There are a few other features embedded in reflector.go including publishing streams from Go, downloading or upload blobs, resolving content and more unfinished tools.

This code includes a Go implementations of the LBRY peer protocol, reflector protocol, and DHT.

## Installation

- Install mysql 8 (5.7 might work too)
- add a reflector user and database with password `reflector` with localhost access only
- Create the tables as described [here](https://github.com/lbryio/reflector.go/blob/master/db/db.go#L735) (the link might not update as the code does so just look for the schema in that file)

#### We do not support running reflector.go as a blob receiver, however if you want to run it as a private blobcache you may compile it yourself and run it as following:
```bash
./prism-bin reflector \
--conf="none" \
--disable-uploads=true \
--use-db=false \
--upstream-reflector="reflector.lbry.com" \
--upstream-protocol="http" \
--request-queue-size=200 \
--disk-cache="2GB:/path/to/your/storage/:localdb" \
```

Create a systemd script if you want to run it automatically on startup or as a service.

## Usage

Usage as reflector/blobcache:
```bash
Run reflector server

Usage:
  prism reflector [flags]

Flags:
      --disable-blocklist                 Disable blocklist watching/updating
      --disable-uploads                   Disable uploads to this reflector server
      --disk-cache string                 Where to cache blobs on the file system. format is 'sizeGB:CACHE_PATH:cachemanager' (cachemanagers: localdb/lfuda/lru) (default "100GB:/tmp/downloaded_blobs:localdb")
  -h, --help                              help for reflector
      --http-peer-port int                The port reflector will distribute content from over HTTP protocol (default 5569)
      --http3-peer-port int               The port reflector will distribute content from over HTTP3 protocol (default 5568)
      --mem-cache int                     enable in-memory cache with a max size of this many blobs
      --metrics-port int                  The port reflector will use for prometheus metrics (default 2112)
      --optional-disk-cache string        Optional secondary file system cache for blobs. format is 'sizeGB:CACHE_PATH:cachemanager' (cachemanagers: localdb/lfuda/lru) (this would get hit before the one specified in disk-cache)
      --origin-endpoint string            HTTP edge endpoint for standard HTTP retrieval
      --origin-endpoint-fallback string   HTTP edge endpoint for standard HTTP retrieval if first origin fails
      --receiver-port int                 The port reflector will receive content from (default 5566)
      --request-queue-size int            How many concurrent requests from downstream should be handled at once (the rest will wait) (default 200)
      --tcp-peer-port int                 The port reflector will distribute content from for the TCP (LBRY) protocol (default 5567)
      --upstream-protocol string          protocol used to fetch blobs from another upstream reflector server (tcp/http3/http) (default "http")
      --upstream-reflector string         host:port of a reflector server where blobs are fetched from
      --use-db                            Whether to connect to the reflector db or not (default true)

Global Flags:
      --conf string       Path to config. Use 'none' to disable (default "config.json")
  -v, --verbose strings   Verbose logging for specific components
```

Other uses:

```bash
Prism is a single entry point application with multiple sub modules which can be leveraged individually or together

Usage:
  prism [command]

Available Commands:
  check-integrity check blobs integrity for a given path
  cluster         Start(join) to or Start a new cluster
  decode          Decode a claim value
  dht             Run dht node
  getstream       Get a stream from a reflector server
  help            Help about any command
  peer            Run peer server
  populate-db     populate local database with blobs from a disk storage
  publish         Publish a file
  reflector       Run reflector server
  resolve         Resolve a URL
  send            Send a file to a reflector
  sendblob        Send a random blob to a reflector server
  start           Runs full prism application with cluster, dht, peer server, and reflector server.
  test            Test things
  upload          Upload blobs to S3
  version         Print the version

Flags:
      --conf string       Path to config. Use 'none' to disable (default "config.json")
  -h, --help              help for prism
  -v, --verbose strings   Verbose logging for specific components
```
## Running from Source

This project requires [Go v1.20](https://golang.org/doc/install).

On Ubuntu you can install it with `sudo snap install go --classic`

```
git clone git@github.com:lbryio/reflector.go.git
cd reflector.go
make
./dist/linux_amd64/prism-bin
```

## Contributing

coming soon

## License

This project is MIT licensed.

## Security

We take security seriously. Please contact security@lbry.com regarding any security issues.
Our PGP key is [here](https://lbry.com/faq/pgp-key) if you need it.

## Contact
The primary contact for this project is [@Nikooo777](https://github.com/Nikooo777) (niko-at-lbry.com)
