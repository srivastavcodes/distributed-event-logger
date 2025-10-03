package loadbalancer

import (
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var (
	_ base.PickerBuilder = (*Picker)(nil)
	_ balancer.Picker    = (*Picker)(nil)
)

type Picker struct {
	mu        sync.Mutex
	current   uint64
	leader    balancer.SubConn
	followers []balancer.SubConn
}

// Build sets up the picker - behind the scenes, after grpc passes it a map
// of sub-connections with information about those sub-connections.
func (p *Picker) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	p.mu.Lock()
	defer p.mu.Unlock()

	var followers []balancer.SubConn
	for sc, scInfo := range buildInfo.ReadySCs {
		isLeader := scInfo.Address.
			Attributes.
			Value("is_leader").(bool)
		if isLeader {
			p.leader = sc
		} else {
			followers = append(followers, sc)
		}
	}
	p.followers = followers
	return p
}

// Pick takes in a balancer.PickInfo provided by grpc internally containing
// the rpc's name and context to help the picker know what sub-connections
// to pick. Header metadata can be read from the context.
//
// Returns a balancer.PickResult with the sub-connection to handle the call.
//
// Optionally, you can set a Done callback on the result that the gRPC calls
// when the RPC completes. The callback tells you the RPC's error, trailer
// metadata, and whether there were bytes send and received from the server.
func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var result balancer.PickResult
	switch {
	case len(p.followers) == 0:
		result.SubConn = p.leader
	case strings.Contains(info.FullMethodName, "Produce"):
		result.SubConn = p.leader
	case strings.Contains(info.FullMethodName, "Consume"):
		result.SubConn = p.nextFollower()
	}
	// balancer.ErrNoSubConnAvailable instructs grpc to block the client's RPCs
	// until the picker has an available sub-connection to handle them.
	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}
	return result, nil
}

func (p *Picker) nextFollower() balancer.SubConn {
	var (
		cur = atomic.AddUint64(&p.current, uint64(1))
		ln  = uint64(len(p.followers))
		idx = int(cur % ln)
	)
	return p.followers[idx]
}

func init() {
	var (
		picker Picker
		config base.Config
	)
	builder := base.NewBalancerBuilder(Name, &picker, config)
	balancer.Register(builder)
}
