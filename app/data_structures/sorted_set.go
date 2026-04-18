package data_structures

import (
	"cmp"
	"slices"
)

// ZSetNode represents the (score, member) pair
type ZSetNode struct {
	Score  float64
	Member string
}

// SortedSet uses a slice for ordered operations and a map for O(1) lookups
type SortedSet struct {
	nodes  []ZSetNode
	lookup map[string]float64
}

func NewSortedSet() *SortedSet {
	return &SortedSet{
		nodes:  make([]ZSetNode, 0),
		lookup: make(map[string]float64),
	}
}

// compareZSetNodes is our comparator.
// Returns -1 if a < b, 0 if a == b, +1 if a > b.
func compareZSetNodes(a, b ZSetNode) int {
	if a.Score != b.Score {
		return cmp.Compare(a.Score, b.Score)
	}
	// If scores are tied, sort by member string lexicographically
	return cmp.Compare(a.Member, b.Member)
}

// Add inserts or updates a member in the sorted set
func (z *SortedSet) Add(score float64, member string) int {
	delta := 1
	if oldScore, exists := z.lookup[member]; exists {
		if oldScore == score {
			return 0
		}
		z.Remove(member)
		delta = 0
	}

	newNode := ZSetNode{Score: score, Member: member}

	index, _ := slices.BinarySearchFunc(z.nodes, newNode, compareZSetNodes)

	z.nodes = slices.Insert(z.nodes, index, newNode)
	z.lookup[member] = score
	return delta
}

// Remove deletes a member from the sorted set
func (z *SortedSet) Remove(member string) {
	score, exists := z.lookup[member]
	if !exists {
		return
	}

	targetNode := ZSetNode{Score: score, Member: member}
	index, found := slices.BinarySearchFunc(z.nodes, targetNode, compareZSetNodes)

	if found {
		z.nodes = slices.Delete(z.nodes, index, index+1)
	}
	delete(z.lookup, member)
}

// GetRange returns a slice of nodes between start and stop indices (inclusive)
func (z *SortedSet) GetRange(start, stop int) []ZSetNode {
	length := len(z.nodes)

	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	if start < 0 {
		start = 0
	}
	if start >= length {
		return []ZSetNode{}
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return []ZSetNode{}
	}

	return z.nodes[start : stop+1]
}
