package worker

import "cloudflow/sdk/golang/cloudflow/task"

type TaskOperations interface {
	Run()
	RunTask(tsk task.Task)
}
