package hash

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// ConsistentHash implements a consistent hashing ring for distributing
// tasks across worker nodes with minimal redistribution on topology changes.
// This is inspired by the approach used in Kubernetes' IPVS load balancer.
type ConsistentHash struct {
	mu           sync.RWMutex
	ring         []uint32            // sorted hash values
	nodes        map[uint32]string   // hash -> node ID
	members      map[string]bool     // set of unique node IDs
	virtualNodes int                 // replicas per physical node
}

// NewConsistentHash creates a new consistent hash ring.
// virtualNodes controls the granularity — higher values yield more uniform distribution.
func NewConsistentHash(virtualNodes int) *ConsistentHash {
	if virtualNodes <= 0 {
		virtualNodes = 150
	}
	return &ConsistentHash{
		nodes:        make(map[uint32]string),
		members:      make(map[string]bool),
		virtualNodes: virtualNodes,
	}
}

func (c *ConsistentHash) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// Add inserts a node into the hash ring.
func (c *ConsistentHash) Add(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.members[nodeID] {
		return
	}
	c.members[nodeID] = true

	for i := 0; i < c.virtualNodes; i++ {
		h := c.hashKey(fmt.Sprintf("%s#%d", nodeID, i))
		c.ring = append(c.ring, h)
		c.nodes[h] = nodeID
	}
	sort.Slice(c.ring, func(i, j int) bool { return c.ring[i] < c.ring[j] })
}

// Remove deletes a node from the hash ring.
func (c *ConsistentHash) Remove(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.members[nodeID] {
		return
	}
	delete(c.members, nodeID)

	newRing := make([]uint32, 0, len(c.ring)-c.virtualNodes)
	for _, h := range c.ring {
		if c.nodes[h] != nodeID {
			newRing = append(newRing, h)
		} else {
			delete(c.nodes, h)
		}
	}
	c.ring = newRing
}

// Get returns the node responsible for the given key.
func (c *ConsistentHash) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.ring) == 0 {
		return "", false
	}

	h := c.hashKey(key)
	idx := sort.Search(len(c.ring), func(i int) bool { return c.ring[i] >= h })
	if idx >= len(c.ring) {
		idx = 0
	}
	return c.nodes[c.ring[idx]], true
}

// GetN returns the N closest distinct nodes to the given key.
// Useful for replication or fallback assignment.
func (c *ConsistentHash) GetN(key string, n int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.ring) == 0 {
		return nil
	}

	h := c.hashKey(key)
	idx := sort.Search(len(c.ring), func(i int) bool { return c.ring[i] >= h })

	seen := make(map[string]bool)
	var result []string

	for i := 0; i < len(c.ring) && len(result) < n; i++ {
		pos := (idx + i) % len(c.ring)
		nodeID := c.nodes[c.ring[pos]]
		if !seen[nodeID] {
			seen[nodeID] = true
			result = append(result, nodeID)
		}
	}
	return result
}

// Members returns all registered node IDs.
func (c *ConsistentHash) Members() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	members := make([]string, 0, len(c.members))
	for m := range c.members {
		members = append(members, m)
	}
	return members
}

// Size returns the number of distinct nodes.
func (c *ConsistentHash) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.members)
}
