package pubsub

import (
	"sort"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type deliveryTracker struct {
	duplicates         map[peer.ID]map[string]int // number of duplicate messages received from the peer.
	duplicateLatency   map[peer.ID]map[string]time.Duration
	firstDeliveries    map[peer.ID]map[string]int
	firstDeliveryTime  map[string]time.Time
	chokedPeers        map[peer.ID]map[string]bool
	inboundChokedPeers map[peer.ID]map[string]bool // TODO: Think of a better variable name
	firstIhaves        map[peer.ID]map[string]int
	duplicateIhaves    map[peer.ID]map[string]int
}

func NewDeliveryTracker() *deliveryTracker {
	return &deliveryTracker{
		duplicates:         map[peer.ID]map[string]int{},
		duplicateLatency:   map[peer.ID]map[string]time.Duration{},
		firstDeliveries:    map[peer.ID]map[string]int{},
		firstDeliveryTime:  map[string]time.Time{},
		chokedPeers:        map[peer.ID]map[string]bool{},
		inboundChokedPeers: map[peer.ID]map[string]bool{},
		firstIhaves:        map[peer.ID]map[string]int{},
		duplicateIhaves:    map[peer.ID]map[string]int{},
	}
}

func (d *deliveryTracker) viableForChoking(params GossipSubParams) map[peer.ID][]string {
	peersToChokeByTopic := map[string][]peer.ID{}
	for p, pMap := range d.duplicates {
		for t, v := range pMap {
			totalMessages := d.firstDeliveries[p][t] + v
			duplicatePercentage := float64(v) / float64(totalMessages)
			if duplicatePercentage > params.ChokeDuplicatesThreshold {
				copiedId := p
				peersToChokeByTopic[t] = append(peersToChokeByTopic[t], copiedId)
			}
		}
	}

	for t, peers := range peersToChokeByTopic {
		// Sort slice in descending order.
		sort.Slice(peers, func(i, j int) bool {
			return d.duplicates[peers[i]][t] > d.duplicates[peers[j]][t]
		})
		if len(peers) > params.ChokeChurn {
			peers = peers[:params.ChokeChurn]
		}
		peersToChokeByTopic[t] = peers
	}

	peersToChoke := make(map[peer.ID][]string)
	// Invert Map to be keyed by peer id instead
	for t, peers := range peersToChokeByTopic {
		for _, pid := range peers {
			copiedTopic := t
			peersToChoke[pid] = append(peersToChoke[pid], copiedTopic)
		}
	}

	return peersToChoke
}

func (d *deliveryTracker) viableForUnchoking(params GossipSubParams) map[peer.ID][]string {
	peerMap := map[string][]peer.ID{}
	for p, tmap := range d.chokedPeers {
		for t, _ := range tmap {
			totalMessageCount := float64(d.firstIhaves[p][t] + d.duplicateIhaves[p][t])
			peerThreshold := float64(d.firstIhaves[p][t]) / totalMessageCount
			if params.UnchokeThreshold <= peerThreshold {
				peerMap[t] = append(peerMap[t], p)
			}
		}
	}

	for t, peers := range peerMap {
		// Sort slice in descending order.
		sort.Slice(peers, func(i, j int) bool {
			return d.firstIhaves[peers[i]][t] > d.duplicates[peers[j]][t]
		})
		if len(peers) > params.UnchokeChurn {
			peers = peers[:params.UnchokeChurn]
		}
		peerMap[t] = peers
	}

	peersToUnChoke := make(map[peer.ID][]string)
	// Invert Map to be keyed by peer id instead
	for t, peers := range peerMap {
		for _, pid := range peers {
			copiedTopic := t
			peersToUnChoke[pid] = append(peersToUnChoke[pid], copiedTopic)
		}
	}

	return peersToUnChoke
}

func (d *deliveryTracker) addDuplicate(msg *Message, id string) {
	topic := *msg.Topic
	totalDur := d.duplicateLatency[msg.ReceivedFrom][topic] * time.Duration(d.duplicates[msg.ReceivedFrom][topic])
	msgLatency := time.Since(d.firstDeliveryTime[id])
	totalDur += msgLatency
	peerMap, ok := d.duplicates[msg.ReceivedFrom]
	if !ok {
		peerMap = map[string]int{}
	}
	peerMap[topic]++
	d.duplicates[msg.ReceivedFrom] = peerMap
	peerlatencyMap, ok := d.duplicateLatency[msg.ReceivedFrom]
	if !ok {
		peerlatencyMap = map[string]time.Duration{}
	}
	peerlatencyMap[topic] = totalDur / time.Duration(d.duplicates[msg.ReceivedFrom][topic])
	d.duplicateLatency[msg.ReceivedFrom] = peerlatencyMap
}

func (d *deliveryTracker) addFirstDelivery(msg *Message, id string) {
	topic := *msg.Topic

	d.firstDeliveries[msg.ReceivedFrom][topic]++
	d.firstDeliveryTime[id] = time.Now()
}

func (d *deliveryTracker) chokePeers(chokedPeers map[peer.ID][]string) {
	for p, topics := range chokedPeers {
		peerMap, ok := d.chokedPeers[p]
		if !ok {
			peerMap = map[string]bool{}
		}
		for _, t := range topics {
			exists := peerMap[t]
			if !exists {
				peerMap[t] = true
			}
		}
		d.chokedPeers[p] = peerMap
	}
}

func (d *deliveryTracker) unChokePeers(unchokedPeers map[peer.ID][]string) {
	for p, topics := range unchokedPeers {
		for _, t := range topics {
			delete(d.chokedPeers[p], t)
			delete(d.firstIhaves[p], t)
			delete(d.duplicateIhaves[p], t)
		}
	}
}

func (d *deliveryTracker) chokedFromPeer(pid peer.ID, topic string) {
	cPeer, ok := d.inboundChokedPeers[pid]
	if !ok {
		cPeer = map[string]bool{}
	}
	cPeer[topic] = true
	d.inboundChokedPeers[pid] = cPeer
}

func (d *deliveryTracker) unChokedFromPeer(pid peer.ID, topic string) {
	delete(d.inboundChokedPeers[pid], topic)
}

func (d *deliveryTracker) isChokedFromRemotePeer(pid peer.ID, topic string) bool {
	return d.inboundChokedPeers[pid][topic]
}

func (d *deliveryTracker) isChokedFromLocalPeer(pid peer.ID, topic string) bool {
	return d.chokedPeers[pid][topic]
}
