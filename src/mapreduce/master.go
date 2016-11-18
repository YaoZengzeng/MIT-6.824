package mapreduce

import "container/list"
import "fmt"
import "strconv"

type WorkerInfo struct {
	address string
	// You can add definitions here.
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

func (mr *MapReduce) RunMaster() *list.List {
	// wait for one worker exists at least
	worker, ok := <-mr.registerChannel
	if !ok {
		fmt.Printf("Get first worker regist information error\n")
	}
	mr.Workers[strconv.Itoa(mr.WorkerCount)] = &WorkerInfo{address: worker}
	mr.WorkerCount++

	// get other worker's registration in parallelism
	go func() {
		for {
			worker, ok := <-mr.registerChannel
			if !ok {
				fmt.Printf("Get worker regist information error\n")
				continue
			}
			mr.Workers[strconv.Itoa(mr.WorkerCount)] = &WorkerInfo{address: worker}
			mr.WorkerCount++
		}
	}()

	r := make(chan bool)
	for i := 0; i < mr.nMap; i++ {
		go func(jobNumber int) {
			args := &DoJobArgs{
				File:          mr.file,
				Operation:     Map,
				JobNumber:     jobNumber,
				NumOtherPhase: mr.nReduce,
			}

			var reply DoJobReply
			workerAddress := mr.Workers[strconv.Itoa(jobNumber%mr.WorkerCount)].address
			ok := call(workerAddress, "Worker.DoJob", args, &reply)
			if ok == false {
				fmt.Printf("DoWork: RPC %s DoMapJob error, Job num %d\n", workerAddress, jobNumber)
			} else {
				r <- true
			}
		}(i)
	}

	// Wait for mr.nMap Map tasks to finish
	for i := 0; i < mr.nMap; i++ {
		<-r
	}

	for i := 0; i < mr.nReduce; i++ {
		go func(jobNumber int) {
			args := &DoJobArgs{
				File:          mr.file,
				Operation:     Reduce,
				JobNumber:     jobNumber,
				NumOtherPhase: mr.nMap,
			}

			var reply DoJobReply
			workerAddress := mr.Workers[strconv.Itoa(jobNumber%mr.WorkerCount)].address
			ok := call(workerAddress, "Worker.DoJob", args, &reply)
			if ok == false {
				fmt.Printf("DoWork: RPC %s DoReduceJob error, Job num %d\n", workerAddress, jobNumber)
			} else {
				r <- true
			}
		}(i)
	}

	// Wait for mr.nReduce Reduce tasks to finish
	for i := 0; i < mr.nReduce; i++ {
		<-r
	}

	return mr.KillWorkers()
}
