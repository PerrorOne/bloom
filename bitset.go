package bloom

import (
	"github.com/bits-and-blooms/bitset"
)

type BitSet struct {
	bitSet *bitset.BitSet
}

func NewBitSet(m int64) *BitSet {
	return &BitSet{bitSet: bitset.New(uint(m))}
}

func (b *BitSet) Set(offsets []int64) error {
	for _, offset := range offsets {
		b.bitSet.Set(uint(offset))
	}
	return nil
}

func (b *BitSet) Test(offsets []int64) (bool, error) {
	for _, offset := range offsets {
		if !b.bitSet.Test(uint(offset)) {
			return false, nil
		}
	}

	return true, nil
}
