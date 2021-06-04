// Package thriftpool
package thriftpool

import (
	"context"
	"testing"
)

func TestNewThriftPool(t *testing.T) {
	pool := NewThriftPool("127.0.0.1:9898", 3, 5, 10, 1)
	conn1, err := pool.Get(context.Background())
	if err != nil {
		t.Errorf("The first pool.Get error:%s\n", err.Error())
		return
	}
	conn2, err := pool.Get(context.Background())
	if err != nil {
		t.Errorf("The second pool.Get error:%s\n", err.Error())
		return
	}
	t.Logf("pool status, used:%d, idle:%d\n", pool.GetUsed(), pool.GetIdle())
	err = pool.Put(conn1)
	if err != nil {
		t.Errorf("The second pool.Get error:%s\n", err.Error())
	}
	err = pool.Put(conn2)
	if err != nil {
		t.Errorf("The second pool.Get error:%s\n", err.Error())
	}
	pool.Close()
	t.Logf("Test done")
}