package etcdjobq

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultLockLeaseTTL = 1

type lockOptions struct {
	lockLeaseTTL int64
}

// LockOption is a function on the options for Locker.Lock method.
type LockOption func(*lockOptions)

// WithLockLeaseTTL is a LockOption to set lease time-to-live (in seconds)
// for a lock.
func WithLockLeaseTTL(lockLeaseTTL int64) LockOption {
	return func(opts *lockOptions) {
		opts.lockLeaseTTL = lockLeaseTTL
	}
}

// Locker locks a key with the specified TTL.
type Locker struct {
	client clientWrapper
}

// NewLocker creates a Locker.
func NewLocker(client *clientv3.Client) *Locker {
	return &Locker{client: newRealClientWrapper(client)}
}

// Lock is a lock for a key.
type Lock struct {
	locker  *Locker
	leaseID clientv3.LeaseID
	key     string
}

func buildLockOptions(opts ...LockOption) *lockOptions {
	options := &lockOptions{
		lockLeaseTTL: defaultLockLeaseTTL,
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// Lock locks the key if not already locked by another locker.
// If the key is already locked by another locker, it waits for the lock
// to be released and then locks the key, or it returns if ctx is canceled
// or timed out.
//
// The lock are released when Lock.Unlock is called or after some time
// when the connection to the server is closed or the client died.
func (l *Locker) Lock(ctx context.Context, key, value string,
	opts ...LockOption) (*Lock, error) {

	for {
		lock, err := l.TryLock(ctx, key, value, opts...)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			return lock, nil
		}

		if err := l.client.WatchOnceKey(ctx, key); err != nil {
			return nil, err
		}
	}
}

// TryLock locks the key if not already locked by another locker.
// If the key is already locked by another locker, it returns nil.
//
// The lock are released when Lock.Unlock is called or after some time
// when the connection to the server is closed or the client died.
func (l *Locker) TryLock(ctx context.Context, key, value string,
	opts ...LockOption) (*Lock, error) {

	options := buildLockOptions(opts...)
	leaseID, err := l.client.Grant(ctx, options.lockLeaseTTL)
	if err != nil {
		return nil, fmt.Errorf("failed to create lease for lock, err: %w", err)
	}

	succeeded, err := l.client.PutWithLeaseIfNotExist(ctx, key, value, leaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to create lock key: %s, leaseID: %d, err: %w",
			key, leaseID, err)
	}
	if !succeeded {
		return nil, nil
	}

	if err := l.client.KeepAlive(ctx, leaseID); err != nil {
		return nil, fmt.Errorf("failed to keep alive lock key: %s, leaseID: %d, err: %w",
			key, leaseID, err)
	}
	return &Lock{
		locker:  l,
		leaseID: leaseID,
		key:     key,
	}, nil
}

// Unlock releases the lock.
func (l *Lock) Unlock(ctx context.Context) error {
	if err := l.locker.client.Revoke(ctx, l.leaseID); err != nil {
		return fmt.Errorf("failed to unlock key: %s, leaseID: %d, err: %s",
			l.key, l.leaseID, err)
	}
	return nil
}
