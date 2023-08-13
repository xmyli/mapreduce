package coordinator

import (
	"fmt"
	"log"
	"mapreduce/internal/structs"
	"net"
	"net/rpc"
	"sync"
)

type Coordinator struct {
	mutex sync.Mutex

	port string

	mapCount    int
	reduceCount int

	filenames map[int]string

	idleMapTasks       map[Task]bool
	inProgressMapTasks map[Task]bool
	completedMapTasks  map[Task]bool

	idleReduceTasks       map[Task]bool
	inProgressReduceTasks map[Task]bool
	completedReduceTasks  map[Task]bool
}

type Task struct {
	id     int
	worker int
}

func (c *Coordinator) GetTask(args *structs.GetTaskArgs, reply *structs.GetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.completedMapTasks) >= c.mapCount && len(c.completedReduceTasks) >= c.reduceCount {
		reply.Done = true
		return nil
	}

	for mapTask := range c.idleMapTasks {
		delete(c.idleMapTasks, mapTask)
		mapTask.worker = args.WorkerId
		c.inProgressMapTasks[mapTask] = true
		reply.TaskType = 0
		reply.TaskId = mapTask.id
		reply.MapOrReduceCount = c.reduceCount
		reply.Filename = c.filenames[mapTask.id]
		return nil
	}

	for mapTask := range c.inProgressMapTasks {
		reply.TaskType = 0
		reply.TaskId = mapTask.id
		reply.MapOrReduceCount = c.reduceCount
		reply.Filename = c.filenames[mapTask.id]
		return nil
	}

	if len(c.idleReduceTasks) == 0 && len(c.inProgressReduceTasks) == 0 {
		for i := 0; i < c.reduceCount; i++ {
			task := Task{id: i, worker: -1}
			c.idleReduceTasks[task] = true
		}
	}

	for reduceTask := range c.idleReduceTasks {
		delete(c.idleReduceTasks, reduceTask)
		reduceTask.worker = args.WorkerId
		c.inProgressReduceTasks[reduceTask] = true
		reply.TaskType = 1
		reply.TaskId = reduceTask.id
		reply.MapOrReduceCount = c.mapCount
		return nil
	}

	for reduceTask := range c.inProgressReduceTasks {
		reply.TaskType = 1
		reply.TaskId = reduceTask.id
		reply.MapOrReduceCount = c.mapCount
		return nil
	}

	return nil
}

func (c *Coordinator) CompleteTask(args *structs.CompleteTaskArgs, reply *structs.CompleteTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.TaskType == 0 {
		mapTask := Task{args.TaskId, args.WorkerId}
		delete(c.inProgressMapTasks, mapTask)
		c.completedMapTasks[mapTask] = true
	} else if args.TaskType == 1 {
		reduceTask := Task{args.TaskId, args.WorkerId}
		delete(c.inProgressReduceTasks, reduceTask)
		c.completedReduceTasks[reduceTask] = true
	}

	if len(c.completedMapTasks) >= c.mapCount && len(c.completedReduceTasks) >= c.reduceCount {
		reply.Done = true
		log.Println("Done.")
	}

	return nil
}

func (c *Coordinator) Start() {
	rpcs := rpc.NewServer()
	rpcs.Register(c)
	fmt.Println()
	rpcs.PrintServices()
	fmt.Println()
	listener, err := net.Listen("tcp", ":"+c.port)
	if err != nil {
		log.Fatalln(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err == nil {
				go rpcs.ServeConn(conn)
			} else {
				break
			}
		}
		listener.Close()
	}()
}

func CreateCoordinator(files []string, reduceCount int, port string) *Coordinator {
	coordinator := Coordinator{
		mapCount:    len(files),
		reduceCount: reduceCount,

		port: port,

		filenames: map[int]string{},

		idleMapTasks:       map[Task]bool{},
		inProgressMapTasks: map[Task]bool{},
		completedMapTasks:  map[Task]bool{},

		idleReduceTasks:       map[Task]bool{},
		inProgressReduceTasks: map[Task]bool{},
		completedReduceTasks:  map[Task]bool{},
	}

	count := 0
	for _, filename := range files {
		coordinator.filenames[count] = filename
		mapTask := Task{count, -1}
		coordinator.idleMapTasks[mapTask] = true
		count++
	}

	coordinator.Start()
	return &coordinator
}
