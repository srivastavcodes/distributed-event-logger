package discovery

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestMembership(t *testing.T) {
	m, hdlr := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	require.Eventually(t, func() bool {
		return 2 == len(hdlr.joins) && 3 == len(m[0].Members()) && 0 == len(hdlr.leaves)
	}, 3*time.Second, 250*time.Millisecond)

	require.NoError(t, m[2].Leave())

	require.Eventually(t, func() bool {
		return 2 == len(hdlr.joins) && 3 == len(m[0].Members()) &&
			serf.StatusLeft == m[0].Members()[2].Status && 1 == len(hdlr.leaves)
	}, 3*time.Second, 250*time.Millisecond)

	require.Equal(t, strconv.Itoa(2), <-hdlr.leaves)
}

func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	id := len(members)
	ports := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{"rpc_addr": addr}

	cfg := Config{
		NodeName: strconv.Itoa(id),
		BindAddr: addr,
		Tags:     tags,
	}
	var hdlr handler
	if len(members) == 0 {
		hdlr.joins = make(chan map[string]string, 3)
		hdlr.leaves = make(chan string, 3)
	} else {
		cfg.StartJoinAddrs = []string{
			members[0].BindAddr,
		}
	}
	m, err := NewMembership(&hdlr, cfg)
	require.NoError(t, err)

	return append(members, m), &hdlr
}

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (hdlrs *handler) Join(id, addr string) error {
	if hdlrs.joins != nil {
		hdlrs.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil
}

func (hdlrs *handler) Leave(id string) error {
	if hdlrs.leaves != nil {
		hdlrs.leaves <- id
	}
	return nil
}
