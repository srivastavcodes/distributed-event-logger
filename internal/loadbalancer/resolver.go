package loadbalancer

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

var _ resolver.Builder = (*Resolver)(nil)

const Name = "cubelog"

// Resolver is the type we implement into grpc's resolver.Builder and
// resolver.Resolver interfaces.
type Resolver struct {
	logger zerolog.Logger
	mu     sync.Mutex

	// clientConn is the user's client connection which grpc passes to the resolver
	// for the resolver to update with the servers it discovers.
	clientConn resolver.ClientConn

	// resolverConn is the resolver's own client connection to the server so it can
	// call GetServer() and get the servers.
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
}

// Build receives the data needed to build a resolver that can discover the
// servers (like the target address) and the client connection the resolver
// will update with the servers it discovers. Build sets up a client conn to
// our server so the resolver can call the GetServers() API.
func (r *Resolver) Build(target resolver.Target, conn resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.logger = zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	r.clientConn = conn
	var dialOpts []grpc.DialOption

	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name),
	)
	var err error

	r.resolverConn, err = grpc.NewClient(target.URL.Host, dialOpts...)
	if err != nil {
		return nil, err
	}
	var resNowOpts resolver.ResolveNowOptions
	r.ResolveNow(resNowOpts)
	return r, nil
}

// Scheme returns the resolver's scheme identifier. When you call grpc.NewClient,
// grpc parses out the scheme from the target address you gave it and tries to
// find a resolver that matches, defaulting to it DNS resolver.
//
// For your resolver, you'll format the target address like this: cubelog://address
func (r *Resolver) Scheme() string { return Name }

// init is used to register this Resolver so grpc knows about this resolver when
// it's looking for resolvers that match the target's scheme.
func init() {
	var res Resolver
	resolver.Register(&res)
}

// ResolveNow get called by grpc to resolve the target, discover the servers, and
// update the client connection with the servers.
//
// How your resolver will discover the servers depends on your resolver and the
// service you're working with. For example: a service built for Kubernetes could
// call Kubernetes's API to get the list of endpoints.
// [We] create a grpc client for our service and call the GetServers() API to get
// the cluster's servers.
func (r *Resolver) ResolveNow(_ resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	client := protolog.NewLogClient(r.resolverConn)

	// get cluster information and then set it on client conn attributes with
	// resolver.Address
	var srvReq protolog.GetServersRequest
	res, err := client.GetServers(context.Background(), &srvReq)
	if err != nil {
		r.logger.Error().Err(err).Msg("failed to resolve server")
		return
	}
	var addrs []resolver.Address

	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr:       server.RpcAddr,
			Attributes: attributes.New("is_leader", server.IsLeader),
		})
	}
	// Services can specify how clients should load balance their calls to the
	// service by updating the state with a service config. We update the
	// state with a service config which specifies to use our custom "cubelog"
	// Load Balancer
	_ = r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error().Err(err).Msg("failed to close conn")
	}
}
