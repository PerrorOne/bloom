package bloom

import (
	"fmt"
	"time"
)

const redisMaxLength = 8 * 512 * 1024 * 1024

type Connection interface {
	SetBit(key string, offset int64, value int) error
	GetBit(key string, offset int64) (int, error)
	Expire(key string, expiration time.Duration) error
	Del(keys ...string) error
	Pipeline() Connection
	Exec() error
}

type RedisBitSet struct {
	keyPrefix string
	conn      Connection
	m         uint
}

func NewRedisBitSet(keyPrefix string, m uint, conn Connection) *RedisBitSet {
	return &RedisBitSet{keyPrefix, conn, m}
}

func (r *RedisBitSet) Set(offsets []int64) error {
	c := r.conn.Pipeline()
	for _, offset := range offsets {
		key, thisOffset := r.getKeyOffset(offset)
		err := c.SetBit(key, thisOffset, 1)
		if err != nil {
			return err
		}
	}
	return c.Exec()
}

func (r *RedisBitSet) Test(offsets []int64) (bool, error) {
	for _, offset := range offsets {
		key, thisOffset := r.getKeyOffset(offset)
		bitValue, err := r.conn.GetBit(key, thisOffset)
		if err != nil {
			return false, err
		}
		if bitValue == 0 {
			return false, nil
		}
	}

	return true, nil
}

func (r *RedisBitSet) Expire(seconds int64) error {
	n := uint(0)
	c := r.conn.Pipeline()
	for n <= uint(r.m/redisMaxLength) {
		key := fmt.Sprintf("%s:%d", r.keyPrefix, n)
		n = n + 1
		err := c.Expire(key, time.Duration(seconds)*time.Second)
		if err != nil {
			return err
		}
	}
	return c.Exec()
}

func (r *RedisBitSet) Delete() error {
	n := uint(0)
	keys := make([]string, 0)
	for n <= uint(r.m/redisMaxLength) {
		key := fmt.Sprintf("%s:%d", r.keyPrefix, n)
		keys = append(keys, key)
		n = n + 1
	}
	return r.conn.Del(keys...)
}

func (r *RedisBitSet) getKeyOffset(offset int64) (string, int64) {
	n := offset / redisMaxLength
	thisOffset := offset - n*redisMaxLength
	key := fmt.Sprintf("%s:%d", r.keyPrefix, n)
	return key, thisOffset
}
