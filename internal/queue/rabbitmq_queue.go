package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQQueue struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	queueName  string
	handlers   map[string]JobHandler
	workers    int
	maxRetries int
	mu         sync.RWMutex
	wg         sync.WaitGroup
	stopCh     chan struct{}
	jobs       map[string]*Job
}

func NewRabbitMQQueue(url string, queueName string, workers int, maxRetries int) (Queue, error) {
	// RabbitMQ 연결
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	// 채널 생성
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %v", err)
	}

	// 큐 선언
	_, err = ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %v", err)
	}

	return &RabbitMQQueue{
		conn:       conn,
		channel:    ch,
		queueName:  queueName,
		handlers:   make(map[string]JobHandler),
		workers:    workers,
		maxRetries: maxRetries,
		stopCh:     make(chan struct{}),
		jobs:       make(map[string]*Job),
	}, nil
}

func (q *RabbitMQQueue) Enqueue(ctx context.Context, job *Job) error {
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %v", err)
	}

	err = q.channel.PublishWithContext(ctx,
		"",          // exchange
		q.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         jobBytes,
			DeliveryMode: amqp.Persistent,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish job: %v", err)
	}

	return nil
}

func (q *RabbitMQQueue) Dequeue(ctx context.Context) (*Job, error) {
	msg, ok, err := q.channel.Get(
		q.queueName,
		false, // auto-ack
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get message: %v", err)
	}
	if !ok {
		return nil, ErrNoJobs
	}

	var job Job
	if err := json.Unmarshal(msg.Body, &job); err != nil {
		msg.Reject(false)
		return nil, fmt.Errorf("failed to unmarshal job: %v", err)
	}

	job.DeliveryTag = msg.DeliveryTag

	// 작업 맵에 저장
	q.mu.Lock()
	q.jobs[job.ID] = &job
	q.mu.Unlock()

	return &job, nil

}

func (q *RabbitMQQueue) Complete(ctx context.Context, jobID string) error {
	// DeliveryTag 를 저장해둔 job 을 찾아야 합니다
	// 실제 구현에서는 job 정보를 어딘가에 저장해두어야 합니다
	job, err := q.getJobByID(jobID) // 이 메서드는 구현 필요
	if err != nil {
		return err
	}
	return q.channel.Ack(job.DeliveryTag, false)
}

func (q *RabbitMQQueue) Fail(ctx context.Context, jobID string, err error) error {
	job, getErr := q.getJobByID(jobID)
	if getErr != nil {
		return getErr
	}

	if job.Attempts >= q.maxRetries {
		return q.channel.Reject(job.DeliveryTag, false)
	}
	return q.channel.Reject(job.DeliveryTag, true)
}

func (q *RabbitMQQueue) RegisterHandler(jobType string, handler JobHandler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[jobType] = handler
}

func (q *RabbitMQQueue) Start(ctx context.Context) error {
	for i := 0; i < q.workers; i++ {
		q.wg.Add(1)
		go q.runWorker(ctx, i)
	}
	return nil
}

func (q *RabbitMQQueue) Stop() {
	close(q.stopCh)
	q.wg.Wait()
	q.channel.Close()
	q.conn.Close()
}

func (q *RabbitMQQueue) runWorker(ctx context.Context, id int) {
	defer q.wg.Done()
	log.Printf("Worker %d started", id)

	for {
		select {
		case <-ctx.Done():
			return
		case <-q.stopCh:
			return
		default:
			job, err := q.Dequeue(ctx)
			if err == ErrNoJobs {
				time.Sleep(time.Second)
				continue
			}
			if err != nil {
				log.Printf("Worker %d: Failed to dequeue job: %v", id, err)
				continue
			}

			// 핸들러 찾기
			q.mu.RLock()
			handler, exists := q.handlers[job.Type]
			q.mu.RUnlock()

			if !exists {
				log.Printf("Worker %d: No handler for job type: %s", id, job.Type)
				q.Fail(ctx, job.ID, fmt.Errorf("no handler for job type: %s", job.Type))
				continue
			}

			// 작업 실행
			if err := handler(ctx, job); err != nil {
				log.Printf("Worker %d: Failed to process job %s: %v", id, job.ID, err)
				q.Fail(ctx, job.ID, err)
			} else {
				log.Printf("Worker %d: Completed job %s", id, job.ID)
				q.Complete(ctx, job.ID)
			}
		}
	}
}

func (q *RabbitMQQueue) getJobByID(jobID string) (*Job, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	return job, nil
}
