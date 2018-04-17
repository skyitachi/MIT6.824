package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	// var done sync.WaitGroup
	// for count := 0; count < ntasks; count++{
	// 	var w, filename string
	// 	w = <- registerChan
	// 	if phase == mapPhase{
	// 		filename = mapFiles[count]
	// 	}else{
	// 		filename = ""
	// 	}
	// 	done.Add(1)
	// 	go func(count int){
	// 		defer done.Done()
	// 		call(w, "Worker.DoTask", &DoTaskArgs{jobName, filename, phase, count, n_other}, new(struct{}))
	// 		println("Start ...")
	// 		registerChan <- w
	// 		println("Stop ...")
	// 	}(count)
	// }	
	// done.Wait()
	
	var done sync.WaitGroup
	done.Add(ntasks)
	ch_task := make(chan int, ntasks)
	for i := 0; i < ntasks; i++{
		ch_task <- i
	}
	go func(){
		for {
			w := <- registerChan
			go func(w string){
				for{
					task_id := <- ch_task
					var filename string
					if phase == mapPhase{
						filename = mapFiles[task_id]
					}else{
						filename = ""
					}
					ok := call(w, "Worker.DoTask", &DoTaskArgs{jobName, filename, phase, task_id, n_other}, new(struct{}))
					if ok == false{
						ch_task <- task_id
					}else{
						done.Done()
					}
				}
			}(w)
		}
	}()
	done.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
