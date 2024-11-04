package etcdjobq_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/hnakamur/etcdjobq"
)

func TestQueue(t *testing.T) {
	t.Run("singleWorker", func(t *testing.T) {
		t.Run("oneAtTime", func(t *testing.T) {
			basePrefix := testNamespacePrefix() + t.Name() + "/"
			queuePrefix := basePrefix + "jobQueue/"
			workerPrefix := basePrefix + "jobWorker/"

			heartbeatCh := make(chan struct{}, 1)
			var logs testLogs

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()

				const workerID = "worker1"
				cli := testNewClient(t)
				defer cli.Close()

				ctx := context.Background()
				queue := etcdjobq.NewQueue(cli, queuePrefix, workerPrefix)

				for {
					if _, ok := <-heartbeatCh; !ok {
						return
					}

					job, err := queue.Take(ctx, workerID)
					if err != nil {
						t.Errorf("failed to take job, worker=%s, err=%s", workerID, err)
						return
					}
					if err := job.Finish(ctx); err != nil {
						t.Errorf("failed to finish job, worker=%s, err=%s", workerID, err)
						return
					}
					logs.Append(fmt.Sprintf("finished job: %s", job.Value))
				}
			}()

			cli := testNewClient(t)
			defer cli.Close()

			ctx := context.Background()
			queue := etcdjobq.NewQueue(cli, queuePrefix, workerPrefix)

			if _, err := queue.Enqueue(ctx, "job1"); err != nil {
				t.Fatalf("failed to enqueue job: %s", err)
			}
			heartbeatCh <- struct{}{}

			if _, err := queue.Enqueue(ctx, "job2"); err != nil {
				t.Fatalf("failed to enqueue job: %s", err)
			}
			heartbeatCh <- struct{}{}

			close(heartbeatCh)
			wg.Wait()
			logs.Expect(t,
				"finished job: job1",
				"finished job: job2")
		})
		t.Run("queueThenTake", func(t *testing.T) {
			basePrefix := testNamespacePrefix() + t.Name() + "/"
			queuePrefix := basePrefix + "jobQueue/"
			workerPrefix := basePrefix + "jobWorker/"

			heartbeatCh := make(chan struct{}, 1)
			var logs testLogs

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()

				const workerID = "worker1"
				cli := testNewClient(t)
				defer cli.Close()

				ctx := context.Background()
				queue := etcdjobq.NewQueue(cli, queuePrefix, workerPrefix)

				for {
					if _, ok := <-heartbeatCh; !ok {
						return
					}

					job, err := queue.Take(ctx, workerID)
					if err != nil {
						t.Errorf("failed to take job, worker=%s, err=%s", workerID, err)
						return
					}
					if err := job.Finish(ctx); err != nil {
						t.Errorf("failed to finish job, worker=%s, err=%s", workerID, err)
						return
					}
					logs.Append(fmt.Sprintf("finished job: %s", job.Value))
				}
			}()

			cli := testNewClient(t)
			defer cli.Close()

			ctx := context.Background()
			queue := etcdjobq.NewQueue(cli, queuePrefix, workerPrefix)

			if _, err := queue.Enqueue(ctx, "job1"); err != nil {
				t.Fatalf("failed to enqueue job: %s", err)
			}
			if _, err := queue.Enqueue(ctx, "job2"); err != nil {
				t.Fatalf("failed to enqueue job: %s", err)
			}
			heartbeatCh <- struct{}{}
			heartbeatCh <- struct{}{}
			close(heartbeatCh)
			wg.Wait()
			logs.Expect(t,
				"finished job: job1",
				"finished job: job2")
		})
	})
	t.Run("twoWorkers", func(t *testing.T) {
		t.Run("queueThenLoadBalancing", func(t *testing.T) {
			basePrefix := testNamespacePrefix() + t.Name() + "/"
			queuePrefix := basePrefix + "jobQueue/"
			workerPrefix := basePrefix + "jobWorker/"

			worker1HeartbeatCh := make(chan struct{})
			worker2HeartbeatCh := make(chan struct{})
			var jobIDs []string
			var jobValues []string
			var logs testLogs

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()

				const workerID = "worker1"
				cli := testNewClient(t)
				defer cli.Close()

				ctx := context.Background()
				queue := etcdjobq.NewQueue(cli, queuePrefix, workerPrefix)

				<-worker1HeartbeatCh
				job, err := queue.Take(ctx, workerID)
				if err != nil {
					t.Errorf("failed to take job, worker=%s, err=%s", workerID, err)
					return
				}
				if got, want := job.ID, jobIDs[0]; got != want {
					t.Errorf("%s first job ID mismatch, got=%s, want=%s", workerID, got, want)
				}
				if got, want := job.Value, jobValues[0]; got != want {
					t.Errorf("%s first job value mismatch, got=%s, want=%s", workerID, got, want)
				}
				// log.Printf("%s took job: id=%s, value=%s", workerID, job.ID, job.Value)
				logs.Append(fmt.Sprintf("%s took job: %s", workerID, job.Value))
				// time.Sleep(time.Second)
				if err := job.Finish(ctx); err != nil {
					t.Errorf("failed to finish job, worker=%s, err=%s", workerID, err)
					return
				}
				logs.Append(fmt.Sprintf("%s finished job: %s", workerID, job.Value))
				// time.Sleep(time.Second)
				worker2HeartbeatCh <- struct{}{}

				// time.Sleep(time.Second)
				<-worker1HeartbeatCh
				job, err = queue.Take(ctx, workerID)
				if err != nil {
					t.Errorf("failed to take job, worker=%s, err=%s", workerID, err)
					return
				}
				if got, want := job.ID, jobIDs[2]; got != want {
					t.Errorf("%s second job ID mismatch, got=%s, want=%s", workerID, got, want)
				}
				if got, want := job.Value, jobValues[2]; got != want {
					t.Errorf("%s second job value mismatch, got=%s, want=%s", workerID, got, want)
				}
				// log.Printf("%s took job: id=%s, value=%s", workerID, job.ID, job.Value)
				logs.Append(fmt.Sprintf("%s took job: %s", workerID, job.Value))
				// time.Sleep(time.Second)
				if err := job.Finish(ctx); err != nil {
					t.Errorf("failed to finish job, worker=%s, err=%s", workerID, err)
					return
				}
				logs.Append(fmt.Sprintf("%s finished job: %s", workerID, job.Value))
			}()
			go func() {
				defer wg.Done()

				const workerID = "worker2"
				cli := testNewClient(t)
				defer cli.Close()

				ctx := context.Background()
				queue := etcdjobq.NewQueue(cli, queuePrefix, workerPrefix)

				<-worker2HeartbeatCh
				job, err := queue.Take(ctx, workerID)
				if err != nil {
					t.Errorf("failed to take job, worker=%s, err=%s", workerID, err)
					return
				}
				if got, want := job.ID, jobIDs[1]; got != want {
					t.Errorf("%s first job ID mismatch, got=%s, want=%s", workerID, got, want)
				}
				if got, want := job.Value, jobValues[1]; got != want {
					t.Errorf("%s first job value mismatch, got=%s, want=%s", workerID, got, want)
				}
				// log.Printf("%s took job: id=%s, value=%s", workerID, job.ID, job.Value)
				logs.Append(fmt.Sprintf("%s took job: %s", workerID, job.Value))
				// time.Sleep(time.Second)
				worker1HeartbeatCh <- struct{}{}
				if err := job.Finish(ctx); err != nil {
					t.Errorf("failed to finish job, worker=%s, err=%s", workerID, err)
					return
				}
				// time.Sleep(time.Second)
				logs.Append(fmt.Sprintf("%s finished job: %s", workerID, job.Value))
			}()

			cli := testNewClient(t)
			defer cli.Close()

			ctx := context.Background()
			queue := etcdjobq.NewQueue(cli, queuePrefix, workerPrefix)

			for i := 1; i <= 3; i++ {
				jobValue := fmt.Sprintf("job%d", i)
				if jobID, err := queue.Enqueue(ctx, jobValue); err != nil {
					t.Fatalf("failed to enqueue job: %s", err)
				} else {
					jobIDs = append(jobIDs, jobID)
					jobValues = append(jobValues, jobValue)
					// log.Printf("enqueued job: key=%s, value=%s", jobID, jobValue)
				}
			}
			// time.Sleep(time.Second)
			worker1HeartbeatCh <- struct{}{}

			wg.Wait()
			logs.Expect(t,
				"worker1 took job: job1",
				"worker1 finished job: job1",
				"worker2 took job: job2",
				"worker2 finished job: job2",
				"worker1 took job: job3",
				"worker1 finished job: job3")
		})

	})
}
