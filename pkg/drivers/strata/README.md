# Strata Driver for Kine

Backs kine with [Strata](https://github.com/strata-db/strata) — an embeddable,
S3-durable key-value store. Strata handles WAL management, periodic
checkpoints, and leader election internally, so kine's own leader-election
path is bypassed (`leaderElect=false`).

## Endpoint Format

```
strata://[bucket[/prefix]][?param=value&...]
```

When `bucket` is omitted the node runs in local-only mode (no S3 durability).

## Parameters

| Parameter | Default | Description |
|---|---|---|
| `data-dir` | `/var/lib/strata` | Local directory for the Pebble database and WAL segments. |
| `node-id` | hostname | Stable unique identifier for this node. Must be consistent across restarts. |
| `peer-listen` | — | gRPC listen address for WAL streaming, e.g. `0.0.0.0:3380`. Required to enable multi-node mode (set automatically when `service-name` is provided). |
| `advertise-peer` | `peer-listen` value | Address that other nodes use to reach this node's peer server. Set this when `peer-listen` binds `0.0.0.0` (set automatically when `service-name` is provided). |
| `peer-port` | `3380` | Peer gRPC port used by `service-name` auto-config. |
| `service-name` | — | Kubernetes headless service FQDN. When set, enables multi-node mode automatically: `peer-listen` is set to `0.0.0.0:<peer-port>` and `advertise-peer` is set to `<hostname>.<service-name>:<peer-port>`. Must be a fully-qualified domain name (e.g. `kine.default.svc.cluster.local`) — gRPC does not use DNS search domains. |
| `s3-endpoint` | — | Custom S3-compatible endpoint URL (MinIO, Ceph, etc.). Enables path-style requests automatically. |
| `region` | `us-east-1` | AWS region. |
| `checkpoint-interval` | `15m` | How often the leader writes a full checkpoint to S3. |
| `segment-max-age` | `10s` | Maximum age of a WAL segment before it is rotated and uploaded. |
| `follower-max-retries` | `5` | Consecutive stream failures a follower tolerates before attempting a leader takeover. |

## Examples

**Local only (no S3, single node):**
```
strata://?data-dir=/var/lib/strata
```

**Single node with S3 durability:**
```
strata://my-bucket/k3s?data-dir=/var/lib/strata
```

**Single node, MinIO:**
```
strata://my-bucket/k3s?data-dir=/var/lib/strata&s3-endpoint=http://minio:9000&region=us-east-1
```

**Three-node cluster on Kubernetes (recommended):**
```
strata://my-bucket/k3s?data-dir=/var/lib/strata&service-name=kine.kube-system.svc.cluster.local
```

`node-id` defaults to the pod hostname, and `peer-listen` / `advertise-peer` are derived from `service-name` automatically. All three nodes use the same DSN — no per-node configuration needed.

**Three-node cluster (manual peer config):**
```
# node-a
strata://my-bucket/k3s?data-dir=/var/lib/strata&node-id=node-a&peer-listen=0.0.0.0:3380&advertise-peer=node-a.internal:3380

# node-b
strata://my-bucket/k3s?data-dir=/var/lib/strata&node-id=node-b&peer-listen=0.0.0.0:3380&advertise-peer=node-b.internal:3380

# node-c
strata://my-bucket/k3s?data-dir=/var/lib/strata&node-id=node-c&peer-listen=0.0.0.0:3380&advertise-peer=node-c.internal:3380
```

## AWS Credentials

Credentials are resolved from the standard AWS credential chain:

1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables
2. `~/.aws/credentials` file
3. EC2 instance profile / ECS task role
4. EKS workload identity (IRSA)

No credential configuration is needed inside the DSN.

## S3 Bucket Layout

All objects are stored under the configured prefix:

```
<prefix>/manifest/latest          ← checkpoint manifest (JSON)
<prefix>/checkpoint/<term>/<rev>  ← checkpoint archives
<prefix>/wal/<seq>.seg            ← uploaded WAL segments
<prefix>/election/lock            ← leader election lock
```

Multiple clusters can safely share one bucket by using distinct prefixes.

## Multi-node Behaviour

- All nodes point at the same S3 bucket and prefix.
- On startup each node races to write the S3 election lock. The winner becomes leader; the rest become followers.
- Followers stream the WAL from the leader in real time and serve reads locally.
- Writes sent to a follower are forwarded to the leader transparently.
- If the leader becomes unreachable for `follower-max-retries` consecutive failures, a follower overwrites the lock and takes over.

No external coordination service (etcd, ZooKeeper, Raft quorum) is required.

## Startup Behaviour

On every start, the driver:

1. Reads `manifest/latest` from S3 to find the latest checkpoint.
2. If the local database is absent, downloads and restores the checkpoint.
3. Replays any local WAL segments newer than the checkpoint.
4. Replays any S3 WAL segments newer than the checkpoint and not yet local.
5. Runs leader election (multi-node) or becomes single-node.

A node that loses its disk recovers automatically from S3 with no manual
intervention.
