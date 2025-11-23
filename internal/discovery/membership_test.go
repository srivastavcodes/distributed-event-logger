package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil
}

func (h *handler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}
	return nil
}

func TestMembership(t *testing.T) {
	mem, hdlr := setupMembers(t, nil)

	mem, _ = setupMembers(t, mem)
	mem, _ = setupMembers(t, mem)

	require.Eventually(t, func() bool {
		return 2 == len(hdlr.joins) &&
			0 == len(hdlr.leaves) &&
			3 == len(mem[0].Members())
	}, 3*time.Second, 250*time.Millisecond)

	err := mem[2].Leave()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return 2 == len(hdlr.joins) &&
			1 == len(hdlr.leaves) &&
			3 == len(mem[0].Members()) &&
			serf.StatusLeft == mem[0].Members()[2].Status
	}, 3*time.Second, 250*time.Millisecond)

	require.Equal(t, fmt.Sprintf("%d", 2), <-hdlr.leaves)
}

// setupMembers sets up a new member under a free port and with the member's
// length as the node name so the names are unique. The number's length also
// tell us whether this member is the cluster's initial member or do we have
// a cluster to join.
func setupMembers(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	var (
		id    = len(members)
		ports = dynaport.Get(1)
		addrs = fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		tags  = map[string]string{"rpc_addr": addrs}
	)
	config := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addrs,
		Tags:     tags,
	}
	var hdlr handler
	if len(members) == 0 {
		hdlr.joins = make(chan map[string]string, 3)
		hdlr.leaves = make(chan string, 3)
	} else {
		config.StartJoinAddrs = []string{members[0].BindAddr}
	}
	mem, err := NewMembership(&hdlr, config)
	require.NoError(t, err)

	members = append(members, mem)
	return members, &hdlr
}
