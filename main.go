package main

import (
	"fmt"
	"time"
)

type Job interface {
	Process()
}

type Worker struct {
	WorkerId   int
	Done       chan bool
	JobRunning chan Job
}

func NewWorker(workerID int, jobChan chan Job) *Worker {
	return &Worker{
		WorkerId:   workerID,
		Done:       make(chan bool),
		JobRunning: jobChan,
	}
}

func (w *Worker) Run() {
	fmt.Println("Run worker id ", w.WorkerId)
	go func() {
		for {
			select {
			case job := <-w.JobRunning:
				fmt.Println("Job running ", w.WorkerId)
				job.Process()
			case <-w.Done:
				fmt.Println("Stop worker ", w.WorkerId)
				return
			}
		}
	}()
}

func (w *Worker) StopWorker() {
	w.Done <- true
}

type JobQueue struct {
	Workers    []*Worker
	JobRunning chan Job
	Done       chan bool
}

func NewJobQueue(numOfWorkers int) JobQueue {
	workers := make([]*Worker, numOfWorkers)
	jobRunning := make(chan Job)

	for i := 0; i < numOfWorkers; i++ {
		workers[i] = NewWorker(i, jobRunning)
	}

	return JobQueue{
		Workers:    workers,
		JobRunning: jobRunning,
		Done:       make(chan bool),
	}
}

func (jq *JobQueue) Push(job Job) {
	jq.JobRunning <- job
}

func (jq *JobQueue) Stop() {
	jq.Done <- true
}

func (jq *JobQueue) Start() {
	go func() {
		for i := 0; i < len(jq.Workers); i++ {
			jq.Workers[i].Run()
		}
	}()

	go func() {
		for {
			select {
			case <-jq.Done:
				for i := 0; i < len(jq.Workers); i++ {
					jq.Workers[i].StopWorker()
				}
				return
			}
		}
	}()
}

type Sender struct {
	Email string
}

func (s *Sender) Process() {
	fmt.Println(s.Email)
}

func main() {
	emails := []string{
		"abc+1@gmail.com",
		"abc+2@gmail.com",
		"abc+3@gmail.com",
		"abc+4@gmail.com",
		"abc+5@gmail.com",
		"abc+6@gmail.com",
		"abc+7@gmail.com",
	}

	jobQueue := NewJobQueue(4)
	jobQueue.Start()

	for _, email := range emails {
		sender := Sender{Email: email}
		jobQueue.Push(&sender)
	}

	time.AfterFunc(time.Second*2, func() {
		jobQueue.Stop()
	})

	time.Sleep(time.Second * 6)
}
