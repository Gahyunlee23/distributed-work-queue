package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Gahyunlee23/distributed-work-queue/internal/queue"
	"golang.org/x/net/context"
)

func main() {
	// RabbitMQ 큐 초기화
	rabbitQueue, err := queue.NewRabbitMQQueue(
		"amqp://guest:guest@localhost:5672/",
		"jobs",
		3, // 워커 수
		3, // 최대 재시도 횟수
	)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	// 이메일 작업 핸들러 등록
	rabbitQueue.RegisterHandler("email", func(ctx context.Context, job *queue.Job) error {
		log.Printf("Processing email job: %s", job.ID)
		time.Sleep(2 * time.Second)
		log.Printf("Email sent for job: %s", job.ID)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rabbitQueue.Start(ctx); err != nil {
		log.Fatalf("Failed to start queue: %v", err)
	}

	// 테스트용 작업 추가
	testJob := &queue.Job{
		ID:      "email-1",
		Type:    "email",
		Payload: []byte(`{"to": "test@example.com", "subject": "Test Email"}`),
	}

	if err := rabbitQueue.Enqueue(ctx, testJob); err != nil {
		log.Printf("Failed to enqueue job: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	rabbitQueue.Stop()
	log.Println("Shutdown complete")
}
