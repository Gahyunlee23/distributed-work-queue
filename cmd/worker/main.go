package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Gahyunlee23/distributed-work-queue/internal/config"
	"github.com/Gahyunlee23/distributed-work-queue/internal/queue"
	"github.com/Gahyunlee23/distributed-work-queue/internal/storage"
)

func main() {
	// 1. 설정 로드
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 2. Redis 스토리지 초기화
	redisStorage, _ := storage.NewRedisStorage(cfg.Storage.Redis)

	// 3. Queue 초기화
	redisQueue := queue.NewRedisQueue(
		redisStorage,
		cfg.Queue.WorkerCount,
		cfg.Queue.RetryLimit,
		time.Duration(cfg.Queue.RetryDelay)*time.Second,
	)

	// 4. 작업 핸들러 등록
	redisQueue.RegisterHandler("email", func(ctx context.Context, job *queue.Job) error {
		log.Printf("Processing email job: %s", job.ID)
		// 실제로는 여기서 이메일을 보내는 로직이 들어갈 것입니다
		time.Sleep(2 * time.Second) // 작업 처리를 시뮬레이션
		log.Printf("Email job completed: %s", job.ID)
		return nil
	})

	// 5. 컨텍스트 설정
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 6. 큐 시작
	if err := redisQueue.Start(ctx); err != nil {
		log.Fatalf("Failed to start queue: %v", err)
	}

	// 7. 테스트용 작업 추가
	testJob := &queue.Job{
		ID:      "test-job-1",
		Type:    "email",
		Payload: []byte(`{"to": "stella@gmail.com", "subject": "Stella Test"}`),
	}

	if err := redisQueue.Enqueue(ctx, testJob); err != nil {
		log.Printf("Failed to enqueue job: %v", err)
	}

	// 8. 종료 시그널 처리
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Shutting down...")

	// 9. 정상 종료
	redisQueue.Stop()
	log.Println("Shutdown complete")
}
