# peerd

A peer discovery service for the Sia network. `peerd` crawls the gateway
network, tracking the reliability and location of every peer it finds, and
exposes an HTTP API for retrieving known peers and geographically diverse
bootstrap lists.

## How it works

- Starting from the network's bootstrap peers, `peerd` connects to each known
  peer, performs the gateway handshake, and asks it for more peers.
- Each peer is scanned periodically. Scans record whether the peer was
  reachable, its reported chain height (validated against an explorer), and its
  geolocation.
- On first startup, the GeoLite2-City database is downloaded into the data
  directory.

## Usage

```sh
peerd [flags]
```

| Flag             | Default        | Description                                          |
| ---------------- | -------------- | ---------------------------------------------------- |
| `-network`       | `mainnet`      | the network to use (`mainnet`, `zen`)                |
| `-dir`           | `.`            | the directory to store data                          |
| `-explorer`      | network default | the URL of the explorer API to use                  |
| `-log.level`     | `info`         | the log level                                        |
| `-scan.interval` | `3h`           | the interval between successful scans                |
| `-scan.threads`  | CPU count      | the number of threads to use for scanning            |

The API listens on `:8080`.

### API

- `GET /peers?offset=0&limit=100&output=json` — list known peers with scan
  metadata. `output=text` returns only the addresses.
- `GET /peers/:addr` — details for a single peer.
- `GET /bootstrap?limit=50` — a list of reliable, geographically diverse peers
  suitable for bootstrapping a node.

## Docker

```sh
docker run -d \
  --name peerd \
  -p 8080:8080 \
  -v /data:/data \
  ghcr.io/siafoundation/peers:latest
```

---

`peerd` uses GeoLite2 data created by MaxMind, available from
[https://www.maxmind.com](https://www.maxmind.com).
