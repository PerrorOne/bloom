package bloom_test

import (
	"bloom"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/garyburd/redigo/redis"
)

type MockR struct {
	conn redis.Conn
}

func (m *MockR) SetBit(key string, offset int64, value int) error {
	_, err := m.conn.Do("SETBIT", key, offset, value)
	return err
}

func (m *MockR) GetBit(key string, offset int64) (int, error) {
	return redis.Int(m.conn.Do("GETBIT", key, offset))
}
func (m *MockR) Expire(key string, expiration time.Duration) error {
	_, err := m.conn.Do("Expire", key, expiration.Seconds())
	return err
}

func (m *MockR) Del(keys ...string) error {
	var k []interface{}
	for _, v := range keys {
		k = append(k, v)
	}
	_, err := m.conn.Do("DEL", k...)
	return err
}
func (m *MockR) Pipeline() bloom.Connection {
	return m
}
func (m *MockR) Exec() error {
	return m.conn.Flush()
}

func TestRedisBitSet_New_Set_Test(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Error("Miniredis could not start")
	}
	defer s.Close()

	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", s.Addr()) },
	}
	conn := pool.Get()
	defer conn.Close()

	bitSet := bloom.NewRedisBitSet("test_key", 512, &MockR{conn: conn})
	isSetBefore, err := bitSet.Test([]int64{0})
	if err != nil {
		t.Error("Could not test bitset in redis")
	}
	if isSetBefore {
		t.Error("Bit should not be set")
	}
	err = bitSet.Set([]int64{512})
	if err != nil {
		t.Error("Could not set bitset in redis")
	}
	isSetAfter, err := bitSet.Test([]int64{512})
	if err != nil {
		t.Error("Could not test bitset in redis")
	}
	if !isSetAfter {
		t.Error("Bit should be set")
	}
	err = bitSet.Expire(3600)
	if err != nil {
		t.Errorf("Error adding expiration to bitset: %v", err)
	}
	err = bitSet.Delete()
	if err != nil {
		t.Errorf("Error cleaning up bitset: %v", err)
	}
}
