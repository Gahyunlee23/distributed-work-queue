package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Gahyunlee23/distributed-work-queue/internal/storage"
)

// RedisQueue struct
type RedisQueue struct {
	storage    storage.Storage
	workers    int
	maxRetries int
	retryDelay time.Duration
	handlers   map[string]JobHandler
	mu         sync.RWMutex
	wg         sync.WaitGroup
	stopCh     chan struct{}
}

func NewRedisQueue(storage storage.Storage, workers, maxRetries int, retryDelay time.Duration) Queue {
	return &RedisQueue{
		storage:    storage,
		workers:    workers,
		maxRetries: maxRetries,
		retryDelay: retryDelay,
		handlers:   make(map[string]JobHandler),
		stopCh:     make(chan struct{}),
	}
}

func (q *RedisQueue) RegisterHandler(jobType string, handler JobHandler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[jobType] = handler
}

func (q *RedisQueue) Enqueue(ctx context.Context, job *Job) error {
	// marshal job to json
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return err
	}

	// save jobs
	if err := q.storage.Set(ctx, "job:"+job.ID, jobBytes, 0); err != nil {
		return err
	}

	// add job id to queue
	return q.storage.Set(ctx, "queue:pending", []byte(job.ID), 0)
}

func (q *RedisQueue) Dequeue(ctx context.Context) (*Job, error) {
	// get job id from queue
	jobID, err := q.storage.Get(ctx, "queue:pending")
	if err != nil {
		return nil, err
	}

	// get job info
	jobBytes, err := q.storage.Get(ctx, "job:"+string(jobID))
	if err != nil {
		return nil, err
	}

	var job Job
	if err := json.Unmarshal(jobBytes, &job); err != nil {
		return nil, err
	}

	return &job, nil
}

func (q *RedisQueue) Complete(ctx context.Context, jobID string) error {
	// get job info
	jobBytes, err := q.storage.Get(ctx, "job:"+jobID)
	if err != nil {
		return err
	}

	var job Job
	if err := json.Unmarshal(jobBytes, &job); err != nil {
		return err
	}

	job.Status = JobStatusCompleted

	// save updated job
	jobBytes, _ = json.Marshal(job)
	return q.storage.Set(ctx, "job:"+jobID, jobBytes, 0)
}

func (q *RedisQueue) Fail(ctx context.Context, jobID string, errMsg error) error {
	jobBytes, err := q.storage.Get(ctx, "job:"+jobID)
	if err != nil {
		return err
	}

	var job Job
	if err := json.Unmarshal(jobBytes, &job); err != nil {
		return err
	}

	job.Attempts++
	job.Error = errMsg.Error()

	// 재시도 로직 추가
	if job.Attempts >= q.maxRetries {
		job.Status = JobStatusFailed
	} else {
		job.Status = JobStatusRetrying
		// 재시도 큐에 추가
		go func() {
			time.Sleep(q.retryDelay)
			retryCtx := context.Background()
			job.Status = JobStatusPending
			q.Enqueue(retryCtx, &job)
		}()
	}

	jobBytes, _ = json.Marshal(job)
	return q.storage.Set(ctx, "job:"+jobID, jobBytes, 0)
}

func (q *RedisQueue) Start(ctx context.Context) error {
	for i := 0; i < q.workers; i++ {
		q.wg.Add(1)
		go q.runWorker(ctx, i)
	}
	return nil
}

func (q *RedisQueue) Stop() {
	close(q.stopCh)
	q.wg.Wait()
}

func (q *RedisQueue) runWorker(ctx context.Context, id int) {
	defer q.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-q.stopCh:
			return
		default:
			// 작업 가져오기
			job, err := q.Dequeue(ctx)
			if err != nil {
				// 작업이 없으면 잠시 대기
				time.Sleep(time.Second)
				continue
			}

			// 핸들러 찾기
			q.mu.RLock()
			handler, exists := q.handlers[job.Type]
			q.mu.RUnlock()

			if !exists {
				// 핸들러가 없으면 작업 실패 처리
				q.Fail(ctx, job.ID, fmt.Errorf("no handler for job type: %s", job.Type))
				continue
			}

			// 작업 실행
			if err := handler(ctx, job); err != nil {
				q.Fail(ctx, job.ID, err)
				continue
			}

			// 작업 완료 처리
			q.Complete(ctx, job.ID)
		}
	}
}
