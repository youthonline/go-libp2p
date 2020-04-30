package identify

import (
	"github.com/libp2p/go-libp2p-core/network"
)

// IDPush is the protocol.ID of the Identify push protocol. It sends full identify messages containing
// the current state of the peer.
//
// It is in the process of being replaced by identify delta, which sends only diffs for better
// resource utilisation.
const IDPush = "/p2p/id/push/1.1.0"

// LegacyIDPush is the protocol.ID of the previous version of the Identify push protocol,
// which does not support exchanging signed addresses in PeerRecords.
// It is still supported for backwards compatibility if a remote peer does not support
// the current version.
const LegacyIDPush = "/ipfs/id/push/1.0.0"

// pushHandler handles incoming identify push streams. The behaviour is identical to the ordinary identify protocol.
func (ids *IDService) pushHandler(s network.Stream) {
	ids.responseHandler(s)
}
