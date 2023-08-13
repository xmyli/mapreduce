package structs

type GetTaskArgs struct {
	WorkerId int
}

type GetTaskReply struct {
	Done bool

	// 0 = map, 1 = reduce
	TaskType         int
	TaskId           int
	MapOrReduceCount int

	Filename string
}
