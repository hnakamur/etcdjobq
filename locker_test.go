package etcdjobq_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hnakamur/etcdjobq"
)

func TestLocker(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		cluster := startEtcdCluster(t)
		defer cluster.Close()

		activeWorkerKey := t.Name() + "/active-worker"

		heartbeatCh := make(chan struct{}, 1)
		var logs testLogs

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()

			const workerID = "worker1"
			cli := cluster.newClient(t)
			defer cli.Close()

			ctx := context.Background()
			locker := etcdjobq.NewLocker(cli)

			lock, err := locker.Lock(ctx, activeWorkerKey, workerID, etcdjobq.WithLockLeaseTTL(1))
			if err != nil {
				t.Errorf("failed to lock key=%s, worker=%s, err=%s", activeWorkerKey, workerID, err)
				return
			}
			logs.Append(fmt.Sprintf("%s locked", workerID))
			heartbeatCh <- struct{}{}

			time.Sleep(100 * time.Millisecond)
			if err := lock.Unlock(ctx); err != nil {
				t.Errorf("failed to unlock key=%s, worker=%s, err=%s", activeWorkerKey, workerID, err)
				return
			}
			logs.Append(fmt.Sprintf("%s unlocked", workerID))
		}()
		go func() {
			defer wg.Done()

			cli := cluster.newClient(t)
			defer cli.Close()

			const workerID = "worker2"
			ctx := context.Background()
			locker := etcdjobq.NewLocker(cli)

			<-heartbeatCh
			lock, err := locker.Lock(ctx, activeWorkerKey, workerID, etcdjobq.WithLockLeaseTTL(1))
			if err != nil {
				t.Errorf("failed to lock key=%s, worker=%s, err=%s", activeWorkerKey, workerID, err)
				return
			}
			logs.Append(fmt.Sprintf("%s locked", workerID))

			time.Sleep(100 * time.Millisecond)
			if err := lock.Unlock(ctx); err != nil {
				t.Errorf("failed to lock key=%s, worker=%s, err=%s", activeWorkerKey, workerID, err)
				return
			}
			logs.Append(fmt.Sprintf("%s unlocked", workerID))
		}()
		wg.Wait()

		logs.Expect(t,
			"worker1 locked",
			"worker1 unlocked",
			"worker2 locked",
			"worker2 unlocked")
	})
	t.Run("closeClientWithoutUnlock", func(t *testing.T) {
		cluster := startEtcdCluster(t)
		defer cluster.Close()

		activeWorkerKey := t.Name() + "/active-worker"

		heartbeatCh := make(chan struct{}, 1)
		var logs testLogs

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()

			const workerID = "worker1"
			cli := cluster.newClient(t)
			defer cli.Close()

			ctx := context.Background()
			locker := etcdjobq.NewLocker(cli)
			_, err := locker.Lock(ctx, activeWorkerKey, workerID, etcdjobq.WithLockLeaseTTL(1))
			if err != nil {
				t.Errorf("failed to lock key=%s, worker=%s, err=%s", activeWorkerKey, workerID, err)
				return
			}
			logs.Append(fmt.Sprintf("%s locked", workerID))
			heartbeatCh <- struct{}{}

			time.Sleep(100 * time.Millisecond)
		}()
		go func() {
			defer wg.Done()

			const workerID = "worker2"
			cli := cluster.newClient(t)
			defer cli.Close()

			ctx := context.Background()
			locker := etcdjobq.NewLocker(cli)

			<-heartbeatCh
			lock, err := locker.Lock(ctx, activeWorkerKey, workerID, etcdjobq.WithLockLeaseTTL(1))
			if err != nil {
				t.Errorf("failed to lock key=%s, worker=%s, err=%s", activeWorkerKey, workerID, err)
				return
			}
			logs.Append(fmt.Sprintf("%s locked", workerID))

			time.Sleep(100 * time.Millisecond)
			if err := lock.Unlock(ctx); err != nil {
				t.Errorf("failed to lock key=%s, worker=%s, err=%s", activeWorkerKey, workerID, err)
				return
			}
			logs.Append(fmt.Sprintf("%s unlocked", workerID))
		}()
		wg.Wait()

		logs.Expect(t,
			"worker1 locked",
			"worker2 locked",
			"worker2 unlocked")
	})
}

type testLogs struct {
	logs []string
	mu   sync.Mutex
}

func (l *testLogs) Append(log string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, log)
}

func (l *testLogs) Expect(t *testing.T, wantedLogs ...string) {
	if got, want := strings.Join(l.logs, "\n"), strings.Join(wantedLogs, "\n"); got != want {
		t.Errorf("logs mismatch,\n got=\n%v,\nwant=\n%v", got, want)
	}
}
