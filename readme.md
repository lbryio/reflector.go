# Reflector

Production-ready blob reflection, distribution, and caching for Odysee.

This repository provides the components used in production:
- Reflector ingestion server (command name: `reflector`)
- Blob cache/edge server (`blobcache`)
- Uploader to object storage (`upload`)

Other commands exist in the tree for historical/legacy reasons and are not supported.

## How it works (at a glance)
- Ingestion (reflector): accepts uploaded blobs, persists them to object storage (e.g., S3/Wasabi) and tracks state in MySQL.
- Distribution: serves blobs over HTTP/HTTP3/Peer. Blobcaches can be deployed in front of the origin to reduce latency and egress.
- Caching (blobcache): layered disk caches backed by HTTP(S) origins (e.g., S3 endpoints), with optional local DB metadata for capacity/eviction.

All services are started by the `prism` binary and are configured via YAML files loaded from a configuration directory.

## Supported commands
The following are the only supported commands for production use:

- Reflector ingestion: `prism reflector`
  - Flags: `--receiver-port` (default 5566), `--metrics-port` (default 2112), `--disable-blocklist`
  - Loads `reflector.yaml` from the config directory.

- Blob cache: `prism blobcache`
  - Flags: `--metrics-port` (default 2112), `--disable-blocklist`
  - Loads `blobcache.yaml` from the config directory.

- Uploader: `prism upload PATH`
  - Flags: `--workers`, `--skipExistsCheck`, `--deleteBlobsAfterUpload`
  - Loads `upload.yaml` from the config directory.

Global flag for all commands:
- `--conf-dir` (default `./`): directory containing YAML config files.

## Configuration
Configuration is per-command. The loader reads `<command>.yaml` from `--conf-dir`.

Common sections:
- `servers`: enables HTTP/HTTP3/Peer servers. Keys: `http`, `http3`, `peer`. Each accepts:
  - `port` (int)
  - `max_concurrent_requests` (int, http/http3)
  - `edge_token` (string, http)
  - `address` (string, optional; bind address, omit for all interfaces)
- `store`: defines the storage topology using composable stores. Frequently used:
  - `proxied-s3`: production pattern with a `writer` (DB-backed -> S3/multiwriter) and a `reader` (caching -> disk + HTTP origins).
  - `caching`: layered cache with a `cache` (often `db_backed` -> `disk`) and an `origin` chain (`http`, `http3`, or `ittt` fan-in).
  - `s3`, `disk`, `multiwriter`, `db_backed`, `http`, `http3`, `peer`, `upstream` are also available building blocks.

### Minimal examples
Reflector – conf-dir contains `reflector.yaml`:
```yaml
servers:
  http:
    port: 5569
    max_concurrent_requests: 200
  http3:
    port: 5568
    max_concurrent_requests: 200
  peer:
    port: 5567
store:
  proxied-s3:
    name: s3_read_proxy
    writer:
      db_backed:
        user: reflector
        password: reflector
        database: reflector
        host: localhost
        port: 3306
        access_tracking: 1
        soft_deletes: true
        store:
          s3:
            name: primary
            aws_id: YOUR_KEY
            aws_secret: YOUR_SECRET
            region: us-east-1
            bucket: blobs-bucket
            endpoint: https://s3.yourendpoint.tv
    reader:
      caching:
        cache:
          disk:
            name: local_cache
            mount_point: /mnt/reflector/cache
            sharding_size: 2
        origin:
          http:
            endpoint: https://s3.yourendpoint.tv/blobs-bucket/
            sharding_size: 4
```

Blobcache – conf-dir contains `blobcache.yaml`:
```yaml
servers:
  http:
    port: 5569
    max_concurrent_requests: 200
  http3:
    port: 5568
    max_concurrent_requests: 200
  peer:
    port: 5567
store:
  caching:
    cache:
      db_backed:
        user: reflector
        password: reflector
        database: reflector
        host: localhost
        port: 3306
        has_cap: true
        max_size: 500GB
        store:
          disk:
            name: blobcache
            mount_point: /mnt/blobcache/cache
            sharding_size: 2
    origin:
      http:
        endpoint: https://s3.yourendpoint.tv/blobs-bucket/
        sharding_size: 4
```

Uploader – conf-dir contains `upload.yaml` (points to the same writer/backend as reflector):
```yaml
database:
  user: reflector
  password: reflector
  database: reflector
  host: localhost
  port: 3306
store:
  proxied-s3:
    writer:
      db_backed:
        user: reflector
        password: reflector
        database: reflector
        host: localhost
        port: 3306
        store:
          s3:
            aws_id: YOUR_KEY
            aws_secret: YOUR_SECRET
            region: us-east-1
            bucket: blobs-bucket
            endpoint: https://s3.yourendpoint.tv
```

## Quick start
1) Build
- Requires Go 1.24+
- `make` (binaries in `dist/<platform>/prism-bin`)

2) Run a local blobcache
```bash
./dist/linux_amd64/prism-bin --conf-dir=./ blobcache
```
Place your `blobcache.yaml` in the `--conf-dir` directory.

3) Run reflector ingestion
```bash
./dist/linux_amd64/prism-bin --conf-dir=./ reflector --receiver-port=5566 --metrics-port=2112
```

4) Upload blobs
```bash
./dist/linux_amd64/prism-bin --conf-dir=./ upload /path/to/blobs \
  --workers=4 --skipExistsCheck
```

## Notes
- Only reflector, blobcache, and upload are supported. All other commands are legacy and may be removed in the future.
- Metrics are exposed on the configured `--metrics-port` at `/metrics` (Prometheus format).
- MySQL is required when using DB-backed stores (e.g., ingestion writer, capacity-aware caches).

## Security
If you discover a security issue, please email security@lbry.com. Our PGP key is available at https://lbry.com/faq/pgp-key.

## License
MIT License. See LICENSE.

## Contact
The primary contact for this project is [@Nikooo777](https://github.com/Nikooo777)
