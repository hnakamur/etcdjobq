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

	queue *Queue
	lock  *Lock
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
		// log.Printf("Enqueue key=%s, value=%s, succeeded=%v, err=%v", key, value, succeeded, err)
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
	defaultWorkerLockTTL      = 1
	defaultQueueRangeGetLimit = 10
)

type takeOptions struct {
	workerLockLeaseTTL int64
	queueRangeGetLimit int64
}

// TakeOption is a function on the options for Queue.Take method.
type TakeOption func(*takeOptions)

// WithWorkerLockLeaseTTL is a TakeOption to set worker lock lease time-to-live
// in seconds.
func WithWorkerLockLeaseTTL(workerLockLeaseTTL int64) TakeOption {
	return func(opts *takeOptions) {
		opts.workerLockLeaseTTL = workerLockLeaseTTL
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
// if ctx is canceled or timed out.
func (q *Queue) Take(ctx context.Context, workerID string,
	opts ...TakeOption) (job *Job, err error) {

	for {
		job, err = q.TakeNoWait(ctx, workerID, opts...)
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
		workerLockLeaseTTL: defaultWorkerLockTTL,
		queueRangeGetLimit: defaultQueueRangeGetLimit,
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// TakeNoWait takes a non-running job from the queue and lock it.
// It returns nil if there is no available job.
func (q *Queue) TakeNoWait(ctx context.Context, workerID string,
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
				WithLockLeaseTTL(options.workerLockLeaseTTL))
			if err != nil {
				return nil, err
			}
			// log.Printf("TakeNoWait jobID=%s, value=%s, lockKey=%s, lock=%v, err=%v", jobID, string(kv.Value), key, lock, err)
			if lock != nil {
				value := string(kv.Value)
				return &Job{
					ID:    jobID,
					Value: value,
					lock:  lock,
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

	if err := j.lock.Unlock(ctx); err != nil {
		return err
	}
	return nil
}
