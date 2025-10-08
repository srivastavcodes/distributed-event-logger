# Distributed Event Logger

A small distributed commit log service. Nodes form a cluster, elect a leader (Raft), replicate log entries, and let clients produce
and consume records over gRPC.

## Features

- Raft-backed replicated log (append only)
- Log segmented on disk (store + index per segment)
- Membership + gossip via Serf
- One TCP port multiplexed for Raft + gRPC (cmux)
- Custom gRPC name resolver + load balancer (leader writes / follower reads)
- Streaming Produce + streaming Consume
- Snapshots + restore (FSM snapshotting)
- Optional TLS for intra-cluster + client connections

## Layout

- `internal/log` low-level log: segments, index (mmap), store (length-prefixed), raft integration
- `internal/discovery` Serf membership; joins trigger Raft voter adds
- `internal/server` gRPC service (Produce / Consume / Streams / GetServers)
- `internal/agent` wires everything: mux, raft log, server, discovery
- `internal/loadbalancer` custom resolver + picker (routes Produce to leader; Consume round-robins followers)
- `protolog/v1` protobuf API (records, requests, responses)

## Log Model

Each record:

- `Offset` monotonically increasing
- Stored as: [8-byte length]
  Segments roll when index or store size hits configured max.
  Indexes map relative offsets -> store positions.

## Raft Integration

- FSM applies Append requests
- LogStore backed by same segment system (re-uses append/read)
- Snapshot = full log bytes stream
- Restore rebuilds segments from snapshot
- Bootstrap flag elects initial single-node leader

## gRPC & LB

- Custom resolver scheme: `cubelog://host:port`
- Resolver calls `GetServers` to fetch cluster view
- Picker:
    - Produce / ProduceStream -> leader
    - Consume / ConsumeStream -> followers (round-robin)
    - Fallback to leader if no followers

## TLS (optional)

Provide:

- `ServerTLSConfig` for incoming
- `PeerTLSConfig` for Raft dialing

## Testing

The implementation includes comprehensive tests for each component:

```bash
  # Run tests on whole packages at once.
  make test PKG=(package name) 

# Run specific tests for a singular component.
  make test <filename> 
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
