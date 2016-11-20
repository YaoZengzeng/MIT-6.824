package mapreduce

import "container/list"
import "fmt"
import "strconv"
import "math/rand"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	status bool
}

type JobResult struct {
	jobNum    int
	jobResult bool
	workerId  string
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func doJob(workerId string, workerAddress string, args *DoJobArgs, ret chan JobResult) {
	var reply DoJobReply
	ok := call(workerAddress, "Worker.DoJob", args, &reply)
	ret <- JobResult{jobNum: args.JobNumber, jobResult: ok, workerId: workerId}
}

func (mr *MapReduce) GetWorker() (workerAddress, workerId string) {
	r := rand.Intn(mr.WorkerSum)
	for {
		workerId = strconv.Itoa(r)
		r = (r + 1) % mr.WorkerSum
		mr.RLock()
		if mr.Workers[workerId].status {
			workerAddress = mr.Workers[workerId].address
			mr.RUnlock()
			return
		}
		mr.RUnlock()
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// wait for one worker exists at least
	worker, ok := <-mr.registerChannel
	if !ok {
		fmt.Printf("Get first worker regist information error\n")
	}
	mr.Workers[strconv.Itoa(mr.WorkerSum)] = &WorkerInfo{address: worker, status: true}
	mr.WorkerSum++

	// get other worker's registration in parallelism
	go func() {
		for {
			worker, ok := <-mr.registerChannel
			if !ok {
				fmt.Printf("Get worker regist information error\n")
				continue
			}
			mr.Lock()
			mr.Workers[strconv.Itoa(mr.WorkerSum)] = &WorkerInfo{address: worker, status: true}
			mr.Unlock()
			mr.WorkerSum++
		}
	}()

	r := make(chan JobResult)
	go func() {
		for i := 0; i < mr.nMap; i++ {
			args := &DoJobArgs{
				File:          mr.file,
				Operation:     Map,
				JobNumber:     i,
				NumOtherPhase: mr.nReduce,
			}
			workerAddress, workerId := mr.GetWorker()
			go doJob(workerId, workerAddress, args, r)
		}
	}()

	// Wait for mr.nMap Map tasks to finish
	cnt := 0
	for cnt < mr.nMap {
		result := <-r
		if result.jobResult {
			cnt++
		} else {
			mr.Lock()
			mr.Workers[result.workerId].status = false
			mr.Unlock()

			args := &DoJobArgs{
				File:          mr.file,
				Operation:     Map,
				JobNumber:     result.jobNum,
				NumOtherPhase: mr.nReduce,
			}
			workerAddress, workerId := mr.GetWorker()
			go doJob(workerId, workerAddress, args, r)
		}
	}

	go func() {
		for i := 0; i < mr.nReduce; i++ {
			args := &DoJobArgs{
				File:          mr.file,
				Operation:     Reduce,
				JobNumber:     i,
				NumOtherPhase: mr.nMap,
			}
			workerAddress, workerId := mr.GetWorker()
			go doJob(workerId, workerAddress, args, r)
		}
	}()

	// Wait for mr.nReduce Reduce tasks to finish
	cnt = 0
	for cnt < mr.nReduce {
		result := <-r
		if result.jobResult {
			cnt++
		} else {
			mr.Lock()
			mr.Workers[result.workerId].status = false
			mr.Unlock()

			args := &DoJobArgs{
				File:          mr.file,
				Operation:     Reduce,
				JobNumber:     result.jobNum,
				NumOtherPhase: mr.nMap,
			}
			workerAddress, workerId := mr.GetWorker()
			go doJob(workerId, workerAddress, args, r)
		}
	}

	return mr.KillWorkers()
}
