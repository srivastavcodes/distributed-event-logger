package discovery

import (
	"github.com/hashicorp/serf/serf"
	"github.com/rs/zerolog"
	"net"
)

// Handler represents some component in our service that needs to know
// when a server joins or leaves the cluster.
type Handler interface {
	Join(name string, addr string) error
	Leave(name string) error
}

type Config struct {
	Tags     map[string]string
	NodeName string
	BindAddr string

	// StartJoinAddrs is how you configure new nodes to join an existing
	// cluster.
	StartJoinAddrs []string
}

/*
When you have an existing cluster, and you create a new node that you want
to add to that cluster, you need to point your new node to at-least one
of the nodes now in the cluster. After the new node connects to one of those
nodes in the existing cluster, it'll learn about the rest of the nodes, and
vice versa.
*/

// Membership wraps Serf to provide service discovery and cluster membership
// to our service.
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zerolog.Logger
}

// NewMembership creates a Membership with the required configuration and
// event handler.
func NewMembership(handler Handler, config Config) (*Membership, error) {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	mem := &Membership{Config: config,
		handler: handler,
		logger:  &logger,
	}
	if err := mem.setupSerf(); err != nil {
		return nil, err
	}
	return mem, nil
}

func (m *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()

	// serf listens on this port and address for gossiping.
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port

	// events concerning a node joining or leaving the cluster. Use
	// Members() method for a snapshot of members in cluster.
	m.events = make(chan serf.Event)
	config.EventCh = m.events

	// serf shares these tags to the other nodes in the cluster and
	// should use these tags for simple data that informs the cluster
	// how to handle this node.
	config.Tags = m.Tags
	config.NodeName = m.NodeName

	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}
	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		// this is where this instance of the server will join the cluster.
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// eventHandler runs in loop reading events sent by Serf into the events channel,
// handling each incoming event according to the event's type.
//
// When a node joins or leave the cluster, Serf sends an event to all the nodes,
// including the node that joins or leaves the cluster.
// A check is performed to make sure the event isn't from the local server itself,
// preventing the server acting upon itself -- we don't want the server try and
// replicate itself!
func (m *Membership) eventHandler() {
	// Serf may coalesce multiple member updates in to one event. For example
	// 10 nodes join around the same time; in which case, Serf will send you
	// one join event with 10 members, which is why we iterate over the event
	// channel.
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleLeave(member)
			}
		default:
			// Handle any other event types gracefully
			m.logger.Debug().Any("event", e).Msg("unhandled serf event")
		}
	}
}

// isLocal returns whether the given Serf member is the local member
// by checking the members' names.
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Members returns a point in time snapshot of the cluster's Serf members.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave tells this member to leave the Serf cluster.
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// logError logs the given error and message.
func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error().
		Err(err).
		Str("name", member.Name).
		Str("rpc_addr", member.Tags["rpc_addr"]).Msg(msg)
}

// handleJoin handles join event.
func (m *Membership) handleJoin(member serf.Member) {
	err := m.handler.Join(member.Name, member.Tags["rpc_addr"])
	if err != nil {
		m.logError(err, "failed to join", member)
	}
}

// handleLeave handles leave event.
func (m *Membership) handleLeave(member serf.Member) {
	err := m.handler.Leave(member.Name)
	if err != nil {
		m.logError(err, "failed to leave", member)
	}
}
