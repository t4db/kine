// Package s3 provides a kine backend driver backed by Strata — an embeddable,
// S3-durable key-value store. Strata handles WAL management, periodic
// checkpoints, leader election, and follower replication; this package wires
// those capabilities into the kine Backend interface.
//
// # DSN format
//
//	strata://[bucket[/prefix]][?param=value&...]
//
// When bucket is omitted the node runs in offline mode (local durability only).
//
// # Parameters
//
//	data-dir            Local storage directory (default: /var/lib/strata)
//	node-id             Stable unique node ID (default: hostname)
//	peer-listen         gRPC listen address for WAL streaming, e.g. 0.0.0.0:3380
//	                    Required to enable multi-node mode (set automatically
//	                    when service-name is provided).
//	advertise-peer      Advertised peer address (default: peer-listen value).
//	                    Set automatically when service-name is provided.
//	peer-port           Peer gRPC port used by service-name auto-config (default: 3380)
//	service-name        Kubernetes headless service name. When set, enables
//	                    multi-node mode automatically: peer-listen is set to
//	                    0.0.0.0:<peer-port> and advertise-peer is set to
//	                    <hostname>.<service-name>:<peer-port>.
//	s3-endpoint         Custom S3 endpoint URL (MinIO, Ceph, etc.)
//	region              AWS region (default: us-east-1)
//	checkpoint-interval Checkpoint write interval, e.g. 15m (default: 15m)
//	segment-max-age     WAL segment rotation age, e.g. 10s (default: 10s)
//	follower-max-retries Consecutive stream failures before takeover (default: 5)
//
// # Examples
//
// Single node, local only:
//
//	strata://?data-dir=/var/lib/strata
//
// Single node with S3 durability:
//
//	strata://my-bucket/prefix?data-dir=/var/lib/strata
//
// Three-node cluster:
//
//	strata://my-bucket/prefix?data-dir=/var/lib/strata&node-id=node-a&peer-listen=0.0.0.0:3380&advertise-peer=node-a.internal:3380
package strata

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/k3s-io/kine/pkg/drivers"
	kserver "github.com/k3s-io/kine/pkg/server"
	"github.com/makhov/strata"
	straobj "github.com/makhov/strata/pkg/object"
	"os"
)

func init() {
	drivers.Register("strata", New)
}

// New is the kine driver constructor for the "strata" scheme.
func New(ctx context.Context, _ *sync.WaitGroup, cfg *drivers.Config) (leaderElect bool, b kserver.Backend, err error) {
	nodeCfg, err := parseConfig(ctx, cfg.DataSourceName)
	if err != nil {
		return false, nil, fmt.Errorf("strata driver: parse DSN: %w", err)
	}

	node, err := strata.Open(*nodeCfg)
	if err != nil {
		return false, nil, fmt.Errorf("strata driver: open node: %w", err)
	}

	// leaderElect=false: strata handles its own leader election via S3.
	return false, &backend{node: node}, nil
}

// parseConfig parses the DataSourceName (everything after "strata://") into a
// strata.Config.
func parseConfig(ctx context.Context, dsn string) (*strata.Config, error) {
	// Re-add the scheme so url.Parse handles it correctly.
	u, err := url.Parse("strata://" + dsn)
	if err != nil {
		return nil, err
	}

	q := u.Query()

	cfg := &strata.Config{
		DataDir: q.Get("data-dir"),
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "/var/lib/strata"
	}

	// S3 bucket and prefix from host/path.
	bucket := u.Hostname()
	prefix := strings.TrimPrefix(u.Path, "/")

	if bucket != "" {
		store, err := newS3Store(ctx, bucket, prefix, q.Get("s3-endpoint"), q.Get("region"))
		if err != nil {
			return nil, fmt.Errorf("create S3 store: %w", err)
		}
		cfg.ObjectStore = store
	}

	// Optional fields.
	if v := q.Get("node-id"); v != "" {
		cfg.NodeID = v
	}

	peerPort := q.Get("peer-port")
	if peerPort == "" {
		peerPort = "3380"
	}

	// service-name enables multi-node mode without manual peer address
	// configuration. When set, peer-listen defaults to 0.0.0.0:<peer-port>
	// and advertise-peer defaults to <hostname>.<service-name>:<peer-port>,
	// which is the stable DNS name assigned by a Kubernetes headless service.
	// Both can still be overridden explicitly via peer-listen / advertise-peer.
	if svc := q.Get("service-name"); svc != "" && q.Get("peer-listen") == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("resolve hostname for advertise-peer: %w", err)
		}
		cfg.PeerListenAddr = "0.0.0.0:" + peerPort
		cfg.AdvertisePeerAddr = hostname + "." + svc + ":" + peerPort
	}

	if v := q.Get("peer-listen"); v != "" {
		cfg.PeerListenAddr = v
		cfg.AdvertisePeerAddr = v // default; may be overridden below
	}
	if v := q.Get("advertise-peer"); v != "" {
		cfg.AdvertisePeerAddr = v
	}
	if v := q.Get("checkpoint-interval"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("checkpoint-interval: %w", err)
		}
		cfg.CheckpointInterval = d
	}
	if v := q.Get("segment-max-age"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("segment-max-age: %w", err)
		}
		cfg.SegmentMaxAge = d
	}
	if v := q.Get("follower-max-retries"); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
			return nil, fmt.Errorf("follower-max-retries: %w", err)
		}
		cfg.FollowerMaxRetries = n
	}

	return cfg, nil
}

// newS3Store creates an object.Store backed by the given S3 bucket.
func newS3Store(ctx context.Context, bucket, prefix, endpoint, region string) (straobj.Store, error) {
	opts := []func(*awsconfig.LoadOptions) error{}

	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	} else {
		opts = append(opts, awsconfig.WithRegion("us-east-1"))
	}

	if endpoint != "" {
		opts = append(opts, awsconfig.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint}, nil
			}),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	s3opts := []func(*awss3.Options){}
	if endpoint != "" {
		s3opts = append(s3opts, func(o *awss3.Options) {
			o.UsePathStyle = true
		})
	}

	client := awss3.NewFromConfig(awsCfg, s3opts...)
	return straobj.NewS3Store(client, bucket, prefix), nil
}
