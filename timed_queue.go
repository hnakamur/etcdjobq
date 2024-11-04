package etcdjobq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TimedQueue is the job queue which you can schedule each job at the specified
// time.
type TimedQueue struct {
	client              clientWrapper
	queuePrefix         string
	workerPrefix        string
	timezone            *time.Location
	scheduledTimeFormat string
	now                 func() time.Time
}

// TimedJob represents a running timed job.
type TimedJob struct {
	ScheduledTime time.Time
	ID            string
	Value         string

	queue *TimedQueue
	lock  *Lock
}

// ScheduledTimePrecision is the type for the precision of scheduled times.
type ScheduledTimePrecision int

const (
	// Second represents the second precision of scheduled times.
	Second ScheduledTimePrecision = iota + 1
	// Millisecond represents the millisecond precision of scheduled times.
	Millisecond
	// Microsecond represents the microsecond precision of scheduled times.
	Microsecond
	// Nanosecond represents the nanosecond precision of scheduled times.
	Nanosecond
)

const defaultScheduledTimePrecision = Second

type timedQueueOptions struct {
	scheduledTimePrecision ScheduledTimePrecision
	now                    func() time.Time
}

// TimedQueueOption is a function for an option for NewTimedQueue.
type TimedQueueOption func(*timedQueueOptions)

// WithScheduleTimePrecision is a TimedQueueOption to set the precision for
// scheduled times of jobs.
func WithScheduleTimePrecision(prec ScheduledTimePrecision) TimedQueueOption {
	return func(opts *timedQueueOptions) {
		opts.scheduledTimePrecision = prec
	}
}

// WithNowFuncForDebug is a TimedQueueOption to set the function to get
// the current.
// This should not be used in production.
// It is meant for debug purpose.
func WithNowFuncForDebug(now func() time.Time) TimedQueueOption {
	return func(opts *timedQueueOptions) {
		opts.now = now
	}
}

// NewTimedQueue creates a TimedQueue.
func NewTimedQueue(client *clientv3.Client, queuePrefix, workerPrefix string,
	timezone *time.Location, opts ...TimedQueueOption) *TimedQueue {

	options := timedQueueOptions{
		scheduledTimePrecision: defaultScheduledTimePrecision,
		now:                    time.Now,
	}
	for _, opt := range opts {
		opt(&options)
	}

	var scheduledTimeFormat string
	switch options.scheduledTimePrecision {
	case Second:
		scheduledTimeFormat = "20060102T150405"
	case Millisecond:
		scheduledTimeFormat = "20060102T150405.000"
	case Microsecond:
		scheduledTimeFormat = "20060102T150405.000000"
	case Nanosecond:
		scheduledTimeFormat = "20060102T150405.000000000"
	}

	return &TimedQueue{
		client:              newRealClientWrapper(client),
		queuePrefix:         queuePrefix,
		workerPrefix:        workerPrefix,
		timezone:            timezone,
		scheduledTimeFormat: scheduledTimeFormat,
	}
}

// Enqueue create a new job with the specified value.
// The issued job ID is returned.
func (q *TimedQueue) Enqueue(ctx context.Context, t time.Time, value string) (jobID string, err error) {
	for {
		jobID, err = q.issueJobID(t)
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

const jobIDScheduledTimeSeperator = "_"

func (q *TimedQueue) issueJobID(t time.Time) (jobID string, err error) {
	tStr := t.In(q.timezone).Format(q.scheduledTimeFormat)
	uuid, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return tStr + jobIDScheduledTimeSeperator + uuid.String(), nil
}

// ErrInvalidFormattedTimedJobID is the error returned by TimedQueue.Take or
// TimedQueue.TakeNoWait when the job ID is in invalid format.
//
// This should not happen in correct use of this package.
var ErrInvalidFormattedTimedJobID = errors.New("invalid formatted timed job ID")

func (q *TimedQueue) getScheduledTimeFromJobID(jobID string) (time.Time, error) {
	tStr, _, ok := strings.Cut(jobID, jobIDScheduledTimeSeperator)
	if !ok {
		return time.Time{}, ErrInvalidFormattedTimedJobID
	}
	t, err := time.ParseInLocation(q.scheduledTimeFormat, tStr, q.timezone)
	if err != nil {
		return time.Time{}, ErrInvalidFormattedTimedJobID
	}
	return t, nil
}

// Take takes a non-running job from the queue and lock it.
// If there is no available job, it waits for one, or it returns
// if ctx is canceled or timed out.
func (q *TimedQueue) Take(ctx context.Context, workerID string,
	opts ...TakeOption) (job *TimedJob, err error) {

	for {
		job, scheduledTime, err := q.takeForTimeNoWait(ctx, q.now(), workerID, opts...)
		if err != nil {
			return nil, err
		}
		if job != nil {
			return job, nil
		}

		if err := q.client.WatchOnceTwoPrefixesAndTime(ctx, q.queuePrefix, q.workerPrefix, scheduledTime); err != nil {
			return nil, err
		}
	}
}

// TakeNoWait takes a non-running job from the queue and lock it.
// It returns nil as job and the first scheduled time if there is no ready job.
func (q *TimedQueue) TakeNoWait(ctx context.Context, workerID string,
	opts ...TakeOption) (job *TimedJob, firstScheduledTime time.Time, err error) {

	return q.takeForTimeNoWait(ctx, q.now(), workerID, opts...)
}

func (q *TimedQueue) takeForTimeNoWait(ctx context.Context, now time.Time,
	workerID string, opts ...TakeOption) (job *TimedJob, firstScheduledTime time.Time, err error) {

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
			return nil, time.Time{}, fmt.Errorf("get key: %s, err: %w", key, err)
		}
		if len(kvs.Kvs) == 0 {
			return nil, time.Time{}, nil
		}

		for ; i < len(kvs.Kvs); i++ {
			kv := kvs.Kvs[i]
			jobID := strings.TrimPrefix(string(kv.Key), q.queuePrefix)
			scheduledTime, err := q.getScheduledTimeFromJobID(jobID)
			if err != nil {
				return nil, time.Time{}, err
			}
			if scheduledTime.After(now) {
				return nil, scheduledTime, nil
			}

			key := q.workerPrefix + jobID
			lock, err := (&Locker{client: q.client}).TryLock(ctx, key, workerID,
				WithLockLeaseTTL(options.workerLockLeaseTTL))
			if err != nil {
				return nil, time.Time{}, err
			}
			// log.Printf("TakeNoWait jobID=%s, value=%s, lockKey=%s, lock=%v, err=%v", jobID, string(kv.Value), key, lock, err)
			if lock != nil {
				value := string(kv.Value)
				return &TimedJob{
					ID:            jobID,
					Value:         value,
					ScheduledTime: scheduledTime,
					lock:          lock,
					queue:         q,
				}, scheduledTime, nil
			}
		}
		if !kvs.More {
			return nil, time.Time{}, nil
		}
		lastKey = string(kvs.Kvs[len(kvs.Kvs)-1].Key)
	}
}

// Finish deletes the job from the queue and release the lock for it.
func (j *TimedJob) Finish(ctx context.Context) error {
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
