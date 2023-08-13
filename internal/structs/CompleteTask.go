package structs

type CompleteTaskArgs struct {
	TaskType int
	TaskId   int
	WorkerId int
}

type CompleteTaskReply struct {
	Done bool
}
