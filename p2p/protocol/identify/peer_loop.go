package identify

import (
	"context"
	"errors"
	ggio "github.com/gogo/protobuf/io"
	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"

	pb "github.com/libp2p/go-libp2p/p2p/protocol/identify/pb"
)

type peerHandler struct {
	ids *IDService

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	pid peer.ID

	lastIdMsg *pb.Identify

	addrChangeCh  chan struct{}
	protoChangeCh chan struct{}
}

func newPeerHandler(pid peer.ID, ids *IDService) *peerHandler {
	ctx, cancel := context.WithCancel(context.Background())

	ph := &peerHandler{
		ids:           ids,
		ctx:           ctx,
		cancel:        cancel,
		pid:           pid,
		addrChangeCh:  make(chan struct{}, 1),
		protoChangeCh: make(chan struct{}, 1),
	}

	ph.wg.Add(1)
	ph.loop()
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
		case <-ph.addrChangeCh:
			dp, _ := ph.openStream([]string{IDPush, LegacyIDPush})
			// failed to open a push stream
			if dp == nil {
				continue
			}

			mes := &pb.Identify{}
			ph.ids.populateMessage(mes, dp.Conn(), protoSupportsPeerRecords(dp.Protocol()))
			if err := ph.writePush(dp, mes); err != nil {
				continue
			}
			ph.lastIdMsg = mes

		case <-ph.protoChangeCh:
			mes := ph.mkDelta()
			// no delta to send
			if mes == nil || (len(mes.AddedProtocols) == 0 && len(mes.RmProtocols) == 0) {
				continue
			}

			ds, _ := ph.openStream([]string{IDDelta})
			// failed to open a delta stream
			if ds == nil {
				continue
			}

			// update our identify snapshot for this peer if by applying the delta to it
			// if the delta was successfully sent.
			if err := ph.writeDelta(ds, mes); err == nil {
				for _, p1 := range mes.RmProtocols {
					for j, p2 := range ph.lastIdMsg.Protocols {
						if p2 == p1 {
							ph.lastIdMsg.Protocols[j] = ph.lastIdMsg.Protocols[len(ph.lastIdMsg.Protocols)-1]
							ph.lastIdMsg.Protocols = ph.lastIdMsg.Protocols[:len(ph.lastIdMsg.Protocols)-1]
						}
					}
				}

				for _, p := range mes.AddedProtocols {
					ph.lastIdMsg.Protocols = append(ph.lastIdMsg.Protocols, p)
				}
			}

		case <-ph.ctx.Done():
			return
		}
	}
}

func (ph *peerHandler) openStream(protos []string) (network.Stream, error) {
	pstore := ph.ids.Host.Peerstore()

	// avoid the unnecessary stream if the peer does not support the protocol.
	if sup, err := pstore.SupportsProtocols(ph.pid, protos...); err != nil && len(sup) == 0 {
		return nil, errors.New("protocol not supported")
	}

	// negotiate a stream without opening a new connection as we should already have a connection.
	ctx, cancel := context.WithTimeout(ph.ctx, 30*time.Second)
	defer cancel()
	ctx = network.WithNoDial(ctx, "should already have connection")

	// newstream will open a stream on the first protocol the remote peer supports from the among
	// the list of protocols passed to it.
	s, err := ph.ids.Host.NewStream(ctx, ph.pid, protocol.ConvertFromStrings(protos)...)
	if err != nil {
		log.Debugf("error opening delta stream to %s: %s", ph.pid, err.Error())
		return nil, err
	}

	return s, err
}

func (ph *peerHandler) mkDelta() *pb.Delta {
	if ph.lastIdMsg == nil {
		return nil
	}

	old := ph.lastIdMsg.GetProtocols()
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

func (ph *peerHandler) writeDelta(s network.Stream, mes *pb.Delta) error {
	defer helpers.FullClose(s)
	c := s.Conn()
	if err := ggio.NewDelimitedWriter(s).WriteMsg(&pb.Identify{Delta: mes}); err != nil {
		log.Warnf("%s error while sending delta update to %s: %s", IDDelta, c.RemotePeer(), c.RemoteMultiaddr())
		return err
	}
	log.Debugf("%s sent delta update to %s: %s", IDDelta, c.RemotePeer(), c.RemoteMultiaddr())
	return nil
}

func (ph *peerHandler) writePush(s network.Stream, mes *pb.Identify) error {
	defer helpers.FullClose(s)
	c := s.Conn()
	w := ggio.NewDelimitedWriter(s)
	if err := w.WriteMsg(mes); err != nil {
		return err
	}
	log.Debugf("%s sent message to %s %s", ID, c.RemotePeer(), c.RemoteMultiaddr())
	return nil
}
