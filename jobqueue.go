package etcdjobq

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var ErrJobAlreadyDeleted = errors.New("job already deleted")

type Queue struct {
	client       *clientv3.Client
	queuePrefix  string
	workerPrefix string
}

type Job struct {
	ID    string
	Value string
	Lock  *Lock

	queue *Queue
}

func New(client *clientv3.Client, queuePrefix, workerPrefix string) *Queue {
	return &Queue{
		client:       client,
		queuePrefix:  queuePrefix,
		workerPrefix: workerPrefix,
	}
}

func (q *Queue) Enqueue(ctx context.Context, value string) (jobID string, err error) {
	for {
		jobID, err = q.issueJobID()
		if err != nil {
			return "", err
		}
		key := q.queuePrefix + jobID
		resp, err := q.client.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(key), "=", 0)).
			Then(clientv3.OpPut(key, value)).
			Commit()
		if err != nil {
			return "", err
		}
		if resp.Succeeded {
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

type TakeOption func(*takeOptions)

func WithWorkerLockTTL(workerTTL int64) TakeOption {
	return func(opts *takeOptions) {
		opts.workerLockTTL = workerTTL
	}
}

func WithQueueRangeGetLimit(queueRangeGetLimit int64) TakeOption {
	return func(opts *takeOptions) {
		opts.queueRangeGetLimit = queueRangeGetLimit
	}
}

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

		q.waitQueueOrWorkerChange(ctx)
	}
}

func (q *Queue) waitQueueOrWorkerChange(ctx context.Context) {
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	queueWatchCh := q.client.Watch(ctx2, q.queuePrefix, clientv3.WithPrefix())
	workerWatchCh := q.client.Watch(ctx2, q.workerPrefix, clientv3.WithPrefix())
	select {
	case <-queueWatchCh:
	case <-workerWatchCh:
	case <-ctx.Done():
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
		getResp, err := q.client.Get(ctx, key,
			clientv3.WithRange(clientv3.GetPrefixRangeEnd(q.queuePrefix)),
			clientv3.WithLimit(options.queueRangeGetLimit))
		if err != nil {
			return nil, err
		}
		if getResp.Count == 0 {
			return nil, nil
		}

		for ; i < len(getResp.Kvs); i++ {
			kv := getResp.Kvs[i]
			jobID := strings.TrimPrefix(string(kv.Key), q.queuePrefix)
			key := q.workerPrefix + jobID
			lock, err := NewLocker(q.client).TryLock(ctx, key, workerID,
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
		if !getResp.More {
			return nil, nil
		}
		lastKey = string(getResp.Kvs[len(getResp.Kvs)-1].Key)
	}
}

func (j *Job) Finish(ctx context.Context) error {
	q := j.queue
	jobKey := q.queuePrefix + j.ID
	delResp, err := q.client.Delete(ctx, jobKey)
	if err != nil {
		return err
	}
	if delResp.Deleted == 0 {
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

type LockOption func(*lockOptions)

func WithLockTTL(lockTTL int64) LockOption {
	return func(opts *lockOptions) {
		opts.lockTTL = lockTTL
	}
}

type Locker struct {
	client *clientv3.Client
}

func NewLocker(client *clientv3.Client) *Locker {
	return &Locker{client: client}
}

type Lock struct {
	KeepAliveCh <-chan *clientv3.LeaseKeepAliveResponse

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

func (l *Locker) TryLock(ctx context.Context, key, value string,
	opts ...LockOption) (*Lock, error) {

	options := buildLockOptions(opts...)
	grantResp, err := l.client.Grant(ctx, options.lockTTL)
	if err != nil {
		return nil, err
	}
	leaseID := grantResp.ID

	txnResp, err := l.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, value, clientv3.WithLease(leaseID))).
		Commit()
	if err != nil {
		return nil, err
	}
	if txnResp.Succeeded {
		kaCh, err := l.client.KeepAlive(ctx, leaseID)
		if err != nil {
			return nil, err
		}
		return &Lock{
			KeepAliveCh: kaCh,
			locker:      l,
			leaseID:     leaseID,
		}, nil
	}
	return nil, nil
}

func (l *Lock) Unlock(ctx context.Context) error {
	if _, err := l.locker.client.Revoke(ctx, l.leaseID); err != nil {
		return err
	}
	return nil
}
