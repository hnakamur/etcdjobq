// Package etcdjobq provides a simple job queue using etcd.
package etcdjobq

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ErrJobAlreadyDeleted is the error returned by Job.Finish when the task
// is already deleted. It is likey a programming error.
var ErrJobAlreadyDeleted = errors.New("job already deleted")

// Queue is the job queue.
type Queue struct {
	client       clientWrapper
	queuePrefix  string
	workerPrefix string
}

// Job represents a running job.
type Job struct {
	ID    string
	Value string
	Lock  *Lock

	queue *Queue
}

// NewQueue creates a Queue.
func NewQueue(client *clientv3.Client, queuePrefix, workerPrefix string) *Queue {
	return &Queue{
		client:       newRealClientWrapper(client),
		queuePrefix:  queuePrefix,
		workerPrefix: workerPrefix,
	}
}

// Enqueue create a new job with the specified value.
// The issued job ID is returned.
func (q *Queue) Enqueue(ctx context.Context, value string) (jobID string, err error) {
	for {
		jobID, err = q.issueJobID()
		if err != nil {
			return "", err
		}
		key := q.queuePrefix + jobID
		succeeded, err := q.client.PutIfNotExist(ctx, key, value)
		if err != nil {
			return "", err
		}
		if succeeded {
			return jobID, nil
		}
	}
}

func (q *Queue) issueJobID() (jobID string, err error) {
	uuid, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return uuid.String(), nil
}

const (
	defaultWorkerLockTTL      = 10
	defaultQueueRangeGetLimit = 100
)

type takeOptions struct {
	workerLockTTL      int64
	queueRangeGetLimit int64
}

// TakeOption is a function on the options for Queue.Take method.
type TakeOption func(*takeOptions)

// WithWorkerLockTTL is a TakeOption to set worker lock TTL in seconds.
func WithWorkerLockTTL(workerLockTTL int64) TakeOption {
	return func(opts *takeOptions) {
		opts.workerLockTTL = workerLockTTL
	}
}

// WithQueueRangeGetLimit is a TakeOption to set the limit for range get
// of jobs in the queue.
func WithQueueRangeGetLimit(queueRangeGetLimit int64) TakeOption {
	return func(opts *takeOptions) {
		opts.queueRangeGetLimit = queueRangeGetLimit
	}
}

// Take takes a non-running job from the queue and lock it.
// If there is no available job, it waits for one, or it returns
// if ctx is canceled or deadline exceeded.
func (q *Queue) Take(ctx context.Context, workerID string,
	opts ...TakeOption) (job *Job, err error) {

	for {
		job, err = q.takeNoWait(ctx, workerID, opts...)
		if err != nil {
			return nil, err
		}
		if job != nil {
			return job, nil
		}

		if err := q.client.WatchOnceTwoPrefixes(ctx, q.queuePrefix, q.workerPrefix); err != nil {
			return nil, err
		}
	}
}

func buildTakeOptions(opts ...TakeOption) *takeOptions {
	options := &takeOptions{
		workerLockTTL:      defaultWorkerLockTTL,
		queueRangeGetLimit: defaultQueueRangeGetLimit,
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

func (q *Queue) takeNoWait(ctx context.Context, workerID string,
	opts ...TakeOption) (job *Job, err error) {

	options := buildTakeOptions(opts...)
	lastKey := ""
	for {
		i := 0
		var key string
		if lastKey == "" {
			key = q.queuePrefix
		} else {
			key = lastKey
			i++
		}
		kvs, err := q.client.GetFromStartKeyWithPrefixAndLimit(ctx, key,
			q.queuePrefix, options.queueRangeGetLimit)
		if err != nil {
			return nil, fmt.Errorf("get key: %s, err: %w", key, err)
		}
		if len(kvs.Kvs) == 0 {
			return nil, nil
		}

		for ; i < len(kvs.Kvs); i++ {
			kv := kvs.Kvs[i]
			jobID := strings.TrimPrefix(string(kv.Key), q.queuePrefix)
			key := q.workerPrefix + jobID
			lock, err := (&Locker{client: q.client}).TryLock(ctx, key, workerID,
				WithLockTTL(options.workerLockTTL))
			if err != nil {
				return nil, err
			}
			if lock != nil {
				value := string(kv.Value)
				return &Job{
					ID:    jobID,
					Value: value,
					Lock:  lock,
					queue: q,
				}, nil
			}
		}
		if !kvs.More {
			return nil, nil
		}
		lastKey = string(kvs.Kvs[len(kvs.Kvs)-1].Key)
	}
}

// Finish deletes the job from the queue and release the lock for it.
func (j *Job) Finish(ctx context.Context) error {
	q := j.queue
	jobKey := q.queuePrefix + j.ID
	deleted, err := q.client.Delete(ctx, jobKey)
	if err != nil {
		return err
	}
	if deleted == 0 {
		return ErrJobAlreadyDeleted
	}

	if err := j.Lock.Unlock(ctx); err != nil {
		return err
	}
	return nil
}

const defaultLockTTL = 10

type lockOptions struct {
	lockTTL int64
}

// LockOption is a function on the options for Locker.Lock method.
type LockOption func(*lockOptions)

// WithLockTTL is a LockOption to set TTL for a lock.
func WithLockTTL(lockTTL int64) LockOption {
	return func(opts *lockOptions) {
		opts.lockTTL = lockTTL
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
}

func buildLockOptions(opts ...LockOption) *lockOptions {
	options := &lockOptions{
		lockTTL: defaultLockTTL,
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// Lock locks the key if not already locked by another locker.
// If the key is already locked by another locker, it waits for the lock
// to be released and then locks the key, or it returns if ctx is canceled
// or deadline exceeded.
//
// The lock are automatically released after the TTL specified with
// WithLockTTL or the default TTL (10 seconds).
//
// To keep the lock longer than that, you need to call Lock.Renew periodically.
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
// The lock are automatically released after the TTL specified with
// WithLockTTL or the default TTL (10 seconds).
//
// To keep the lock longer than that, you need to call Lock.Renew periodically.
func (l *Locker) TryLock(ctx context.Context, key, value string,
	opts ...LockOption) (*Lock, error) {

	options := buildLockOptions(opts...)
	leaseID, err := l.client.Grant(ctx, options.lockTTL)
	if err != nil {
		return nil, err
	}

	succeeded, err := l.client.PutWithLeaseIfNotExist(ctx, key, value, leaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to TryLock: key: %s, err: %w", key, err)
	}
	if succeeded {
		return &Lock{
			locker:  l,
			leaseID: leaseID,
		}, nil
	}
	return nil, nil
}

// Renew extends the lock TTL.
func (l *Lock) Renew(ctx context.Context) error {
	if err := l.locker.client.KeepAliveOnce(ctx, l.leaseID); err != nil {
		return err
	}
	return nil
}

// Unlock releases the lock.
func (l *Lock) Unlock(ctx context.Context) error {
	if err := l.locker.client.Revoke(ctx, l.leaseID); err != nil {
		return err
	}
	return nil
}
