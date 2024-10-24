package queue

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type JobStatus string

// JobHandler 작업을 처리하는 함수의 타입을 정의합니다
type JobHandler func(context.Context, *Job) error

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusRetrying  JobStatus = "retrying"
)

type Job struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Payload     []byte    `json:"payload"`
	Status      JobStatus `json:"status"`
	Attempts    int       `json:"attempts"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Error       string    `json:"error,omitempty"`
	DeliveryTag uint64    `json:"-"`
}

func NewJob(jobType string, payload []byte) *Job {
	return &Job{
		ID:          uuid.New().String(),
		Type:        jobType,
		Payload:     payload,
		Status:      JobStatusPending,
		Attempts:    0,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Error:       "",
		DeliveryTag: 0,
	}
}
