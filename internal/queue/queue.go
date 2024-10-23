package queue

type Queue interface {
	Enqueue(job *Job) error
	Dequeue() (*Job, error)
	Complete(jobID string) error
	Fail(jobID string, err error) error
}
