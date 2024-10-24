package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrNoJobs = errors.New("no jobs available")
var ErrJobNotFound = errors.New("job not found")

// Queue interface
type Queue interface {
	Enqueue(ctx context.Context, job *Job) error
	Dequeue(ctx context.Context) (*Job, error)
	Complete(ctx context.Context, jobID string) error
	Fail(ctx context.Context, jobID string, err error) error
	RegisterHandler(jobType string, handler JobHandler)
	Start(ctx context.Context) error
	Stop()
}

// MemoryQueue structure
type MemoryQueue struct {
	mu       sync.RWMutex
	jobs     map[string]*Job       // 모든 작업 저장
	pending  []string              // 대기 중인 작업 ID들
	handlers map[string]JobHandler // 작업 타입별 핸들러
	workers  int                   // 워커 수
}

func NewMemoryQueue(workers int) Queue {
	return &MemoryQueue{
		jobs:     make(map[string]*Job),
		pending:  make([]string, 0),
		handlers: make(map[string]JobHandler),
		workers:  workers,
	}
}

// Enqueue implementation
func (q *MemoryQueue) Enqueue(ctx context.Context, job *Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.jobs[job.ID] = job
	q.pending = append(q.pending, job.ID)
	return nil
}

// Dequeue implementation
func (q *MemoryQueue) Dequeue(ctx context.Context) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) == 0 {
		return nil, ErrNoJobs
	}

	// get the first job
	jobID := q.pending[0]
	q.pending = q.pending[1:] // removing the first job

	return q.jobs[jobID], nil
}

// Complete implementation
func (q *MemoryQueue) Complete(ctx context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if job, exists := q.jobs[jobID]; exists {
		job.Status = JobStatusCompleted
		return nil
	}
	return ErrJobNotFound
}

// Fail implementation
func (q *MemoryQueue) Fail(ctx context.Context, jobID string, err error) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if job, exists := q.jobs[jobID]; exists {
		job.Status = JobStatusFailed
		job.Error = err.Error()
		return nil
	}
	return ErrJobNotFound
}
func (q *MemoryQueue) RegisterHandler(jobType string, handler JobHandler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// 핸들러 맵이 없으므로 구조체에 먼저 추가해야 함
	if q.handlers == nil {
		q.handlers = make(map[string]JobHandler)
	}
	q.handlers[jobType] = handler
}

func (q *MemoryQueue) Start(ctx context.Context) error {
	// 워커 관련 필드들도 추가해야 함
	if q.workers <= 0 {
		q.workers = 1 // 기본값 설정
	}

	for i := 0; i < q.workers; i++ {
		go q.runWorker(ctx)
	}
	return nil
}

func (q *MemoryQueue) Stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	// 필요한 정리 작업 수행
}

// 워커 실행을 위한 보조 메서드
func (q *MemoryQueue) runWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// 작업 가져오기
			job, err := q.Dequeue(ctx)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}

			// 핸들러 찾기
			q.mu.RLock()
			handler, exists := q.handlers[job.Type]
			q.mu.RUnlock()

			if !exists {
				q.Fail(ctx, job.ID, fmt.Errorf("no handler for job type: %s", job.Type))
				continue
			}

			// 작업 실행
			if err := handler(ctx, job); err != nil {
				q.Fail(ctx, job.ID, err)
			} else {
				q.Complete(ctx, job.ID)
			}
		}
	}
}
