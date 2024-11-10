package etcdjobq_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hnakamur/etcdjobq"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestLocker(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		etcd := startEmbeddedEtcd(t, "2379")
		defer etcd.Close()

		activeWorkerKey := testNamespacePrefix() + t.Name() + "/active-worker"

		heartbeatCh := make(chan struct{}, 1)
		var logs testLogs

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()

			const workerID = "worker1"
			cli := testNewClient(t)
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

			cli := testNewClient(t)
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
		etcd := startEmbeddedEtcd(t, "2379")
		defer etcd.Close()

		activeWorkerKey := testNamespacePrefix() + t.Name() + "/active-worker"

		heartbeatCh := make(chan struct{}, 1)
		var logs testLogs

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()

			const workerID = "worker1"
			cli := testNewClient(t)
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
			cli := testNewClient(t)
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

func testNamespacePrefix() string {
	prefix, ok := os.LookupEnv("ETCDJOBQ_TEST_NAMESPACE_PREFIX")
	if ok {
		return prefix
	}
	return "etcqjobqTest/"
}

func startEmbeddedEtcd(t *testing.T, port string) *embed.Etcd {
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	lcurl, err := url.Parse("http://localhost:" + port)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ListenClientUrls = []url.URL{*lcurl}

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("failed to start embedded etcd: %v", err)
	}

	select {
	case <-e.Server.ReadyNotify():
		t.Log("Embedded etcd server is ready!")
	case <-time.After(5 * time.Second):
		e.Server.Stop() // サーバーを停止
		t.Fatal("Embedded etcd server took too long to start")
	}
	return e
}

func testNewClient(t *testing.T) *clientv3.Client {
	endpointsStr := os.Getenv("ETCDJOBQ_TEST_ENDPOINTS")
	if endpointsStr == "" {
		endpointsStr = "localhost:2379"
	}
	endpoints := strings.Split(endpointsStr, ",")
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("cannot connect to etcd server at %s, err: %v", endpointsStr, err)
	}
	return cli
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
