package mapreduce
import "container/list"
import "fmt"

const (
  MaxTryTimes = 2
)


type WorkerInfo struct {
  address string
  failedtimes int
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) GetFreeWorker() string {
  // update mr.Workers 
  // load balance

  for _, v := range mr.Workers{
    fmt.Printf("select worker: %s. \n", v.address)
    return v.address
  }
  fmt.Printf("no workers..wait from regester ch \n")
  worker := <- mr.registerChannel
  fmt.Printf("worker %s arrived!\n", worker)
  workerinfo := &WorkerInfo{}
  workerinfo.address = worker
  workerinfo.failedtimes = 0
  mr.Workers[worker] = workerinfo    
  return worker
}

func (mr *MapReduce) UpdateWorkers() {

    for {
      fmt.Printf("wait worker regest in UpdateWorker method.....\n")
      worker := <- mr.registerChannel
      workerinfo := &WorkerInfo{}
      workerinfo.address = worker
	  workerinfo.failedtimes = 0
      mr.Workers[worker] = workerinfo    
      fmt.Printf("new worker %s  regested. \n", worker)
    }

}

func (mr *MapReduce) RunMaster() *list.List {
  //read chanal from regester ch and update to mr.Workers 
  go mr.UpdateWorkers()
 
  for i := 0; i < mr.nMap; i++ {
    //chose worker, one first
    
    worker := mr.GetFreeWorker()
    args := &DoJobArgs{}
    args.File = mr.file
    args.Operation = "Map"//map
    args.JobNumber = i
    args.NumOtherPhase = mr.nReduce
    
    var reply DoJobReply
    // call rpc map fun Dojob
    ok := call(worker, "Worker.DoJob", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", worker)
      mr.Workers[worker].failedtimes++
      failedtimes := mr.Workers[worker].failedtimes
	  if failedtimes == MaxTryTimes{
		fmt.Printf("Worker %s failed dispatch %s times, this worker may already deaded.\n", worker, MaxTryTimes)
        delete(mr.Workers, worker)
		
      }
      i--
    }
  }
  for i := 0; i < mr.nReduce; i++ {
    //chose worker
    worker := mr.GetFreeWorker()
    args := &DoJobArgs{}
    args.File = mr.file
    args.Operation = "Reduce" //map
    args.JobNumber = i
    args.NumOtherPhase = mr.nMap
    // call rpc reduce fun Dojob
    var reply DoJobReply
    ok := call(worker, "Worker.DoJob", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", worker)
      mr.Workers[worker].failedtimes++
      failedtimes := mr.Workers[worker].failedtimes
	  if failedtimes == MaxTryTimes{
		fmt.Printf("Worker %s failed dispatch %s times, this worker may already deaded.\n", worker, MaxTryTimes)
        delete(mr.Workers, worker)
      }
      i--
    }
  }
  //wait for done
  fmt.Printf("waiting done")
  //<- mr.DoneChannel
  return mr.KillWorkers()
}
