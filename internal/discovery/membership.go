package discovery

import (
	"fmt"
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type Handler interface {
	Join(name string, addr string) error
	Leave(name string) error
}

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func NewMembership(handler Handler, config Config) (*Membership, error) {
	mem := &Membership{Config: config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := mem.setupSerf(); err != nil {
		return nil, fmt.Errorf("error setting up serf: %w", err)
	}
	return mem, nil
}

func (mem *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", mem.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()

	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port

	mem.events = make(chan serf.Event)
	config.EventCh = mem.events

	config.Tags = mem.Tags
	config.NodeName = mem.NodeName

	mem.serf, err = serf.Create(config)
	if err != nil {
		return fmt.Errorf("error creating a new serf instance: %w", err)
	}
	go mem.eventHandler()

	if mem.StartJoinAddrs != nil {
		_, err = mem.serf.Join(mem.StartJoinAddrs, true)
		if err != nil {
			return fmt.Errorf("couldn't contact any node: %w", err)
		}
	}
	return nil
}

func (mem *Membership) eventHandler() {
	for event := range mem.events {
		switch event.EventType() {

		// Serf may coalesce multiple members updates into one event. For example: say ten nodes
		// join around the same time; in that case, Serf will send you one join event with ten
		// members, so that's why we iterate over the event's members.
		case serf.EventMemberJoin:
			for _, member := range event.(serf.MemberEvent).Members {
				if mem.isLocal(member) {
					continue
				}
				mem.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range event.(serf.MemberEvent).Members {
				if mem.isLocal(member) {
					continue
				}
				mem.handleLeave(member)
			}
		default:
		}
	}
}

func (mem *Membership) handleJoin(member serf.Member) {
	if err := mem.handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		mem.logError(err, "failed to join", member)
	}
}

func (mem *Membership) handleLeave(member serf.Member) {
	if err := mem.handler.Leave(member.Name); err != nil {
		mem.logError(err, "failed to leave", member)
	}
}

func (mem *Membership) isLocal(member serf.Member) bool {
	return mem.serf.LocalMember().Name == member.Name
}

func (mem *Membership) Members() []serf.Member {
	return mem.serf.Members()
}

func (mem *Membership) Leave() error {
	return mem.serf.Leave()
}

func (mem *Membership) logError(err error, msg string, member serf.Member) {
	mem.logger.Error(msg, zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]))
}
