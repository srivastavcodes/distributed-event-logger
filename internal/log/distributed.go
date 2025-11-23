package log

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/srivastavcodes/distributed-event-logger/protolog/v1"
	"google.golang.org/protobuf/proto"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	dl := &DistributedLog{
		config: config,
	}
	if err := dl.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := dl.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return dl, nil
}

// setupLog creates the log for this server, where this server will store
// the user's records.
func (dl *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")

	err := os.MkdirAll(logDir, 0755)
	if err != nil {
		return err
	}
	dl.log, err = NewLog(logDir, dl.config)
	return err
}

// setupRaft configures and creates the server's Raft instance. A Raft
// instance comprises:
//   - A finite state-machine that applies the command you give Raft.
//   - A log store where Raft stores those command.
//   - A stable store where Raft stores the cluster's configurations-
//     the servers in the cluster, their addresses, and so on.
//   - A snapshot store where Raft stores compact snapshots of its data.
//   - A transport that Raft uses to connect with the server's peers.
func (dl *DistributedLog) setupRaft(dataDir string) error {
	var (
		sm     = &fsm{log: dl.log}
		logDir = filepath.Join(dataDir, "raft", "log")
	)
	err := os.MkdirAll(logDir, 0755)
	if err != nil {
		return err
	}
	logConfig := dl.config
	logConfig.Segment.InitialOffset = 1

	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}
	stableDir := filepath.Join(dataDir, "raft", "stable")

	// A stable store is a key-value store where raft stores important metadata, like
	// the server's current term or the candidate the server voted for.
	stableStore, err := raftboltdb.NewBoltStore(stableDir)
	if err != nil {
		return err
	}
	// retain specifies that we will keep one snapshot.
	retain := 1
	snapshotDir := filepath.Join(dataDir, "raft")

	// Raft snapshots to recover and restore data efficiently when necessary, like
	// if your server went down, and another instance of raft was brought up
	// - rather than stream all the data from the leader, the new server would just
	// restore from the snapshot and get the latest changes from the leader.
	//
	// Ideally you will snapshot raft frequently to minimize the difference between
	// the data in the snapshot and the leader.
	snapshotStore, err := raft.NewFileSnapshotStore(snapshotDir, retain, os.Stderr)
	if err != nil {
		return err
	}
	var (
		maxPool = 5
		timeout = 10 * time.Second
		config  = raft.DefaultConfig()
		network = raft.NewNetworkTransport(dl.config.Raft.StreamLayer, maxPool, timeout, os.Stderr)
	)
	config.LocalID = dl.config.Raft.LocalID
	dl.changeRaftConfig(config)

	dl.raft, err = raft.NewRaft(config, sm, logStore, stableStore, snapshotStore, network)
	if err != nil {
		return err
	}
	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return err
	}
	if dl.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: network.LocalAddr(),
			}},
		}
		err = dl.raft.BootstrapCluster(config).Error()
	}
	return err
}

// Append appends the record to the log. Unlike the store, where we appended
// the record directly to the server's log, we tell raft to apply a command
// that tells the FSM to append the record to the log.
func (dl *DistributedLog) Append(record *protolog.Record) (uint64, error) {
	res, err := dl.apply(AppendRequestType, &protolog.ProduceRequest{
		Record: record,
	})
	if err != nil {
		return 0, nil
	}
	return res.(*protolog.ProduceResponse).Offset, nil
}

// apply wraps raft's api to apply requests and return their responses.
func (dl *DistributedLog) apply(reqType RequestType, req proto.Message) (any, error) {
	var buf bytes.Buffer

	if _, err := buf.Write([]byte{byte(reqType)}); err != nil {
		return nil, err
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	if _, err = buf.Write(data); err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	command := buf.Bytes()

	future := dl.raft.Apply(command, timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

// Read reads the record for the offset from the server's log. When you're okay
// with relaxed consistency, read operations need not go through raft. When you
// need strong consistency, where reads must be up to date with writer, then it
// must go through raft (but reads will become slower and less efficient).
func (dl *DistributedLog) Read(offset uint64) (*protolog.Record, error) {
	return dl.log.Read(offset)
}

// Join adds the server to the Raft cluster. We add every server as a voter, but
// raft supports adding servers as non-voters with AddNonVoter() API.
//
// You'd want non-voter servers useful if you want to if you wanted to replicate
// state to many servers to server read only eventually-consistent-state.
func (dl *DistributedLog) Join(id, addr string) error {
	configFuture := dl.raft.GetConfiguration()

	if err := configFuture.Error(); err != nil {
		return err
	}
	var (
		serverID   = raft.ServerID(id)
		serverAddr = raft.ServerAddress(addr)
	)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// server has already joined
				return nil
			}
			// remove the existing server
			removeFuture := dl.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := dl.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}
	return nil
}

// Leave removes the server from the cluster. Removing the leader will trigger
// a new election.
func (dl *DistributedLog) Leave(id string) error {
	return dl.raft.RemoveServer(raft.ServerID(id), 0, 0).Error()
}

// WaitForLeader blocks until the cluster has elected a leader or times out.
func (dl *DistributedLog) WaitForLeader(timeout time.Duration) error {
	var (
		timeoutch = time.After(timeout)
		ticker    = time.Tick(time.Second)
	)
	for {
		select {
		case <-timeoutch:
			return fmt.Errorf("timed out")
		case <-ticker:
			if addr, _ := dl.raft.LeaderWithID(); addr != "" {
				return nil
			}
		}
	}
}

// Close shuts down the raft instance and closes the local log.
func (dl *DistributedLog) Close() error {
	future := dl.raft.Shutdown()
	if err := future.Error(); err != nil {
		return err
	}
	return dl.log.Close()
}

func (dl *DistributedLog) GetServers() ([]*protolog.Server, error) {
	future := dl.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var servers []*protolog.Server
	for _, server := range future.Configuration().Servers {
		leaderAddress, _ := dl.raft.LeaderWithID()

		servers = append(servers, &protolog.Server{
			Id:       string(server.ID),
			RpcAddr:  string(server.Address),
			IsLeader: leaderAddress == server.Address,
		})
	}
	return servers, nil
}

var _ raft.FSM = (*fsm)(nil)

type RequestType uint8

const AppendRequestType RequestType = 8

type fsm struct {
	log *Log
}

// Apply executes our FSM's command depending on the RequestType. It switches
// over RequestType and calls the respective method that executes the command.
func (f *fsm) Apply(record *raft.Log) any {
	var (
		buf     = record.Data
		reqType = RequestType(buf[0])
	)
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

// applyAppend un-marshals the record from (b) and appends the record on-to
// the log by calling the log's Append method.
//
// Returns protolog.ProduceResponse with the offset of the record on the log,
// back to Raft where we called raft.Apply().
func (f *fsm) applyAppend(b []byte) any {
	var req protolog.ProduceRequest

	err := proto.Unmarshal(b, &req)
	if err != nil {
		return nil
	}
	offset, err := f.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &protolog.ProduceResponse{
		Offset: offset,
	}
}

// Snapshot returns an FSMSnapshot that represent a point-in-time snapshot
// of the FSM's state. In this case that state is our FSM's log, so call
// Reader() to return an io.Reader that will read all the log's data.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

// Persist is called by raft on the FSMSnapshot we created to write its state to
// some sink that, depending on the snapshot store you configured raft with
// could be in-memory, a file, an S3 bucket -- something to store the bytes in.
//
// The benefit of using a shared state store such as S3 bucket would be that it
// would put all burden of reading and writing the snapshot on S3 rather than the
// leader and allow new servers to restore from snapshots without streaming from
// the leader.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return nil
}

func (s *snapshot) Release() {}

// Restore is called by raft to restore an FSM from a snapshot. For example,
// if we lost a server and scaled up a new one, we'd want to restore it FSM.
// The FSM must discard existing state to make sure its state will match the
// leader's replicated state.
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)

	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		switch {
		case errors.Is(err, io.EOF):
			break
		case err != nil:
			return err
		}
		size := int64(enc.Uint64(b))

		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		var record protolog.Record

		err = proto.Unmarshal(buf.Bytes(), &record)
		if err != nil {
			return err
		}
		// Here we reset the log and configure the initial offset to the first record's
		// offset we read from the snapshot so the log's offsets match.
		// Then we read the records in the snapshot and append them to our new log.
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err = f.log.Append(&record); err != nil {
			return err
		}
		buf.Reset()
	}
}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, config Config) (*logStore, error) {
	log, err := NewLog(dir, config)
	if err != nil {
		return nil, err
	}
	return &logStore{Log: log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	record, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = record.Value
	out.Index = record.Offset
	out.Type = raft.LogType(record.Type)
	out.Term = record.Term
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		rec := &protolog.Record{
			Value: record.Data,
			Type:  uint32(record.Type),
			Term:  record.Term,
		}
		if _, err := l.Append(rec); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange removes the records between the offsets - it's to remove
// records that are old or stored in a snapshot.
func (l *logStore) DeleteRange(_, max uint64) error {
	return l.TruncatePrev(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

const RaftRPC = 1

// StreamLayer satisfies the raft.StreamLayer interface and enables encrypted
// communication between servers with serverTLSConfig (for incoming conns) and
// peerTLSConfig (for outgoing conns).
type StreamLayer struct {
	listen          net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

// NewStreamLayer creates a new StreamLayer with the fields instantiated.
func NewStreamLayer(listener net.Listener, serverTLSConfig, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{
		listen:          listener,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

// Dial makes outgoing connections to other raft servers in the cluster.
// When we connect to a server, we write the RaftRPC byte to identify
// the connection type so we can multiplex raft on the same port as our
// Log grpc requests. If we configure the stream layer with a peer TLS
// config, we make a TLS client-side connection.
func (sl *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}

	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	// identify to mux that this is a raft rpc.
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	if sl.peerTLSConfig != nil {
		conn = tls.Client(conn, sl.peerTLSConfig)
	}
	return conn, err
}

// Accept the mirror of Dial. It accepts the incoming connection and read
// the byte that identifies the connection and then creates a server-side
// TLS connection.
func (sl *StreamLayer) Accept() (net.Conn, error) {
	conn, err := sl.listen.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)

	if _, err = conn.Read(b); err != nil {
		return nil, err
	}
	if bytes.Compare([]byte{byte(RaftRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if sl.serverTLSConfig != nil {
		conn = tls.Server(conn, sl.serverTLSConfig)
	}
	return conn, nil
}

// Close closes the listener.
func (sl *StreamLayer) Close() error {
	return sl.listen.Close()
}

// Addr returns the listener's address.
func (sl *StreamLayer) Addr() net.Addr {
	return sl.listen.Addr()
}

// changeRaftConfig is used to change the default raft config.
//
// In our use-case we'll only use it while testing to make the
// tests complete faster.
func (dl *DistributedLog) changeRaftConfig(config *raft.Config) {
	if dl.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = dl.config.Raft.HeartbeatTimeout
	}
	if dl.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = dl.config.Raft.ElectionTimeout
	}
	if dl.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = dl.config.Raft.CommitTimeout
	}
	if dl.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = dl.config.Raft.LeaderLeaseTimeout
	}
}
