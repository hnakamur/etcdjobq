package etcdjobq

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseID = clientv3.LeaseID

type keyAndValues struct {
	Kvs  []keyAndValue
	More bool
}

type keyAndValue struct {
	Key   string
	Value string
}

type clientWrapper interface {
	GetFromStartKeyWithPrefixAndLimit(ctx context.Context, startKey, prefix string, limit int64) (keyAndValues, error)
	PutIfNotExist(ctx context.Context, key, value string) (succeeded bool, err error)
	PutWithLeaseIfNotExist(ctx context.Context, key, value string, leaseID leaseID) (succeeded bool, err error)
	Delete(ctx context.Context, key string) (deleted int64, err error)

	WatchOnceKey(ctx context.Context, key string) error
	WatchOnceTwoPrefixes(ctx context.Context, prefix1, prefix2 string) error
	WatchOnceTwoPrefixesAndTime(ctx context.Context, prefix1, prefix2 string, t time.Time) error

	Grant(ctx context.Context, ttl int64) (leaseID leaseID, err error)
	KeepAlive(ctx context.Context, leaseID leaseID) error
	Revoke(ctx context.Context, leaseID leaseID) error
}

type realClientWrapper struct {
	impl *clientv3.Client
}

func newRealClientWrapper(client *clientv3.Client) *realClientWrapper {
	return &realClientWrapper{impl: client}
}

func (c *realClientWrapper) GetFromStartKeyWithPrefixAndLimit(ctx context.Context, startKey, prefix string, limit int64) (keyAndValues, error) {
	getResp, err := c.impl.Get(ctx, startKey,
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)),
		clientv3.WithLimit(limit))
	if err != nil {
		return keyAndValues{}, err
	}

	var kvs keyAndValues
	if len(getResp.Kvs) > 0 {
		kvs.Kvs = make([]keyAndValue, len(getResp.Kvs))
		for i, kv := range getResp.Kvs {
			kvs.Kvs[i].Key = string(kv.Key)
			kvs.Kvs[i].Value = string(kv.Value)
		}
		kvs.More = getResp.More
	}
	return kvs, nil
}

func (c *realClientWrapper) PutIfNotExist(ctx context.Context, key, value string) (succeeded bool, err error) {
	resp, err := c.impl.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, value)).
		Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

func (c *realClientWrapper) PutWithLeaseIfNotExist(ctx context.Context, key, value string, leaseID leaseID) (succeeded bool, err error) {
	resp, err := c.impl.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, value, clientv3.WithLease(leaseID))).
		Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

func (c *realClientWrapper) Delete(ctx context.Context, key string) (deleted int64, err error) {
	delResp, err := c.impl.Delete(ctx, key)
	if err != nil {
		return 0, err
	}
	return delResp.Deleted, nil
}

func (c *realClientWrapper) WatchOnceKey(ctx context.Context, key string) error {
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	watchCh := c.impl.Watch(ctx2, key)
	select {
	case <-watchCh:
	case <-ctx2.Done():
		return ctx2.Err()
	}
	return nil
}

func (c *realClientWrapper) WatchOnceTwoPrefixes(ctx context.Context, prefix1, prefix2 string) error {
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	queueWatchCh := c.impl.Watch(ctx2, prefix1, clientv3.WithPrefix())
	workerWatchCh := c.impl.Watch(ctx2, prefix2, clientv3.WithPrefix())
	select {
	case <-queueWatchCh:
	case <-workerWatchCh:
	case <-ctx2.Done():
		return ctx2.Err()
	}
	return nil
}

func (c *realClientWrapper) WatchOnceTwoPrefixesAndTime(ctx context.Context, prefix1, prefix2 string, t time.Time) error {
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	timerCtx, timerCancel := context.WithDeadline(context.Background(), t)
	defer timerCancel()

	queueWatchCh := c.impl.Watch(ctx2, prefix1, clientv3.WithPrefix())
	workerWatchCh := c.impl.Watch(ctx2, prefix2, clientv3.WithPrefix())
	select {
	case <-queueWatchCh:
	case <-workerWatchCh:
	case <-timerCtx.Done():
	case <-ctx2.Done():
		return ctx2.Err()
	}
	return nil
}

func (c *realClientWrapper) Grant(ctx context.Context, ttl int64) (leaseID leaseID, err error) {
	grantResp, err := c.impl.Grant(ctx, ttl)
	if err != nil {
		return 0, err
	}
	return grantResp.ID, nil
}

func (c *realClientWrapper) KeepAlive(ctx context.Context, leaseID leaseID) error {
	if _, err := c.impl.KeepAlive(ctx, leaseID); err != nil {
		return err
	}
	return nil
}

func (c *realClientWrapper) Revoke(ctx context.Context, leaseID leaseID) error {
	if _, err := c.impl.Revoke(ctx, leaseID); err != nil {
		return err
	}
	return nil
}
