package worker

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mapreduce/internal/structs"
	"mapreduce/internal/utils"
	"math/rand"
	"net/rpc"
	"os"
)

type Worker struct {
	rpcClient *rpc.Client

	mapFunction   func(string, string) []structs.KeyValue
	reduceFuntion func(string, []string) string
}

func (w *Worker) Start() {
	id := rand.Int()
	done := false
	for !done {
		isDone, taskType, taskId, mapOrReduceCount, filename := w.getTask(id)

		if isDone {
			break
		}

		if taskType == 0 {
			doMapTask(filename, taskId, mapOrReduceCount, w.mapFunction)
		} else if taskType == 1 {
			doReduceTask(taskId, mapOrReduceCount, w.reduceFuntion)
		} else {
			continue
		}

		err, isDone := w.completeTask(id, taskType, taskId)
		if !err {
			done = isDone
		}
	}
	log.Println("Done.")
}

func doMapTask(filename string, mapTaskId int, reduceCount int, mapFunction func(string, string) []structs.KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalln(err)
	}

	file.Close()

	results := mapFunction(filename, string(content))

	buckets := map[int][]structs.KeyValue{}

	for _, keyValue := range results {
		key := keyValue.Key
		bucketId := utils.Hash(key) % reduceCount

		_, exists := buckets[bucketId]
		if !exists {
			buckets[bucketId] = []structs.KeyValue{}
		}

		buckets[bucketId] = append((buckets[bucketId]), keyValue)
	}

	for reduceTaskId, bucket := range buckets {
		file, err := os.Create(fmt.Sprint("temp_", mapTaskId, "_", reduceTaskId))
		if err != nil {
			log.Fatalln(err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalln(err)
			}
		}
		file.Close()
	}
}

func doReduceTask(reduceTaskId int, mapCount int, reduceFunction func(key string, values []string) string) {
	keyValues := map[string][]string{}

	for mapTaskId := 0; mapTaskId < mapCount; mapTaskId++ {
		filename := fmt.Sprint("temp_", mapTaskId, "_", reduceTaskId)
		file, err := os.Open(filename)
		if err != nil {
			log.Println("Failed to open", filename, ", ignoring...")
			continue
		}

		decoder := json.NewDecoder(file)
		for {
			keyValue := structs.KeyValue{}

			err := decoder.Decode(&keyValue)
			if err != nil {
				break
			}

			_, exists := keyValues[keyValue.Key]
			if !exists {
				keyValues[keyValue.Key] = []string{}
			}

			keyValues[keyValue.Key] = append(keyValues[keyValue.Key], keyValue.Value)
		}

		file.Close()
	}

	file, err := os.Create(fmt.Sprint("output_", reduceTaskId))
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	for key, value := range keyValues {
		output := reduceFunction(key, value)
		fmt.Fprintf(file, "%v %v\n", key, output)
	}
}

func (w *Worker) getTask(id int) (bool, int, int, int, string) {
	args := structs.GetTaskArgs{}
	args.WorkerId = id
	reply := structs.GetTaskReply{}

	err := w.rpcClient.Call("Coordinator.GetTask", &args, &reply)
	if err != nil {
		return false, -1, -1, -1, ""
	}

	return reply.Done, reply.TaskType, reply.TaskId, reply.MapOrReduceCount, reply.Filename
}

func (w *Worker) completeTask(id int, taskType int, taskId int) (bool, bool) {
	args := structs.CompleteTaskArgs{}
	args.TaskType = taskType
	args.TaskId = taskId
	args.WorkerId = id

	reply := structs.CompleteTaskReply{}

	err := w.rpcClient.Call("Coordinator.CompleteTask", &args, &reply)
	if err != nil {
		return true, false
	}
	return false, reply.Done
}

func CreateWorker(coordinatorAddress string, coordinatorPort string, mapFunction func(string, string) []structs.KeyValue, reduceFuntion func(string, []string) string) *Worker {
	worker := Worker{}

	rpcClient, err := rpc.Dial("tcp", coordinatorAddress+":"+coordinatorPort)
	if err != nil {
		log.Fatalln(err)
	}
	worker.rpcClient = rpcClient

	worker.mapFunction = mapFunction
	worker.reduceFuntion = reduceFuntion

	return &worker
}
