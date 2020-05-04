package identify

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	pb "github.com/libp2p/go-libp2p/p2p/protocol/identify/pb"

	ggio "github.com/gogo/protobuf/io"
)

var errProtocolNotSupported = errors.New("protocol not supported")

type peerHandler struct {
	ids *IDService

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	pid peer.ID

	msgMu         sync.RWMutex
	idMsgSnapshot *pb.Identify

	pushCh     chan struct{}
	deltaCh    chan struct{}
	evalTestCh chan func() // for testing
}

func newPeerHandler(pid peer.ID, ids *IDService, initState *pb.Identify) *peerHandler {
	ctx, cancel := context.WithCancel(context.Background())

	ph := &peerHandler{
		ids:    ids,
		ctx:    ctx,
		cancel: cancel,
		pid:    pid,

		idMsgSnapshot: initState,

		pushCh:     make(chan struct{}, 1),
		deltaCh:    make(chan struct{}, 1),
		evalTestCh: make(chan func()),
	}

	ph.wg.Add(1)
	go ph.loop()
	return ph
}

func (ph *peerHandler) close() error {
	ph.cancel()
	ph.wg.Wait()
	return nil
}

// per peer loop for pushing updates
func (ph *peerHandler) loop() {
	defer ph.wg.Done()

	for {
		select {
		// our listen addresses have changed, send an IDPush.
		case <-ph.pushCh:
			dp, err := ph.openStream([]string{IDPush})
			if err != nil {
				continue
			}

			mes := &pb.Identify{}
			ph.ids.populateMessage(mes, dp.Conn())

			ph.msgMu.Lock()
			ph.idMsgSnapshot = mes
			ph.msgMu.Unlock()

			ph.sendMessage(dp, mes)

		case <-ph.deltaCh:
			mes := ph.mkDelta()
			if mes == nil || (len(mes.AddedProtocols) == 0 && len(mes.RmProtocols) == 0) {
				continue
			}

			ph.msgMu.Lock()
			// update our identify snapshot for this peer by applying the delta to it
			ph.applyDelta(mes)
			ph.msgMu.Unlock()

			ds, err := ph.openStream([]string{IDDelta})
			if err != nil {
				continue
			}

			ph.sendMessage(ds, &pb.Identify{Delta: mes})

		case fnc := <-ph.evalTestCh:
			fnc()

		case <-ph.ctx.Done():
			return
		}
	}
}

func (ph *peerHandler) applyDelta(mes *pb.Delta) {
	for _, p1 := range mes.RmProtocols {
		for j, p2 := range ph.idMsgSnapshot.Protocols {
			if p2 == p1 {
				ph.idMsgSnapshot.Protocols[j] = ph.idMsgSnapshot.Protocols[len(ph.idMsgSnapshot.Protocols)-1]
				ph.idMsgSnapshot.Protocols = ph.idMsgSnapshot.Protocols[:len(ph.idMsgSnapshot.Protocols)-1]
			}
		}
	}

	for _, p := range mes.AddedProtocols {
		ph.idMsgSnapshot.Protocols = append(ph.idMsgSnapshot.Protocols, p)
	}
}

func (ph *peerHandler) openStream(protos []string) (network.Stream, error) {
	// wait for the other peer to send us an Identify response on "all" connections we have with it
	// so we can look at it's supported protocols and avoid a multistream-select roundtrip to negotiate the protocol
	// if we know for a fact that it dosen't support the protocol.
	conns := ph.ids.Host.Network().ConnsToPeer(ph.pid)
	for _, c := range conns {
		select {
		case <-ph.ids.IdentifyWait(c):
		case <-ph.ctx.Done():
			return nil, ph.ctx.Err()
		}
	}
	pstore := ph.ids.Host.Peerstore()
	if sup, err := pstore.SupportsProtocols(ph.pid, protos...); err != nil && len(sup) == 0 {
		return nil, errProtocolNotSupported
	}

	// negotiate a stream without opening a new connection as we "should" already have a connection.
	ctx, cancel := context.WithTimeout(ph.ctx, 30*time.Second)
	defer cancel()
	ctx = network.WithNoDial(ctx, "should already have connection")

	// newstream will open a stream on the first protocol the remote peer supports from the among
	// the list of protocols passed to it.
	s, err := ph.ids.Host.NewStream(ctx, ph.pid, protocol.ConvertFromStrings(protos)...)
	if err != nil {
		log.Warnf("failed to open %s stream with peer %s, err=%s", protos, ph.pid.Pretty(), err)
		return nil, err
	}

	return s, err
}

func (ph *peerHandler) mkDelta() *pb.Delta {
	old := ph.idMsgSnapshot.GetProtocols()
	curr := ph.ids.Host.Mux().Protocols()

	oldProtos := make(map[string]struct{}, len(old))
	currProtos := make(map[string]struct{}, len(curr))

	for i := range old {
		oldProtos[old[i]] = struct{}{}
	}

	for i := range curr {
		currProtos[curr[i]] = struct{}{}
	}

	var added []string
	var removed []string

	// has it been added ?
	for p := range currProtos {
		if _, ok := oldProtos[p]; !ok {
			added = append(added, p)
		}
	}

	// has it been removed ?
	for p := range oldProtos {
		if _, ok := currProtos[p]; !ok {
			removed = append(removed, p)
		}
	}

	return &pb.Delta{
		AddedProtocols: added,
		RmProtocols:    removed,
	}
}

func (ph *peerHandler) sendMessage(s network.Stream, mes *pb.Identify) error {
	defer helpers.FullClose(s)
	c := s.Conn()
	if err := ggio.NewDelimitedWriter(s).WriteMsg(mes); err != nil {
		log.Warnf("error while sending %s update to %s: err=%s", s.Protocol(), c.RemotePeer(), err)
		return err
	}
	log.Debugf("sent %s update to %s: %s", s.Protocol(), c.RemotePeer(), c.RemoteMultiaddr())
	return nil
}
