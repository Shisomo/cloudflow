package worker

import (
	"cloudflow/sdk/golang/cloudflow/cfmodule"
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"cloudflow/sdk/golang/cloudflow/fileops"
	"cloudflow/sdk/golang/cloudflow/kvops"
	"cloudflow/sdk/golang/cloudflow/task"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"
)

type DumyWorker struct {
	cfmodule.StateCfModule
	FileCfg cf.CFG                     `json:"-"`
	FileOps map[string]fileops.FileOps `json:"-"`
}

func (wk *DumyWorker) Run() {
	cf.Log("start dumy worker:", wk.Name)
	cfmodule.AddModuleAndToList(wk.Kvops, wk.StateCfModule.Uuid, cf.AsKV(wk),
		cf.K_CF_WORKERS, cf.K_STAT_WORK, cf.K_AB_WORKER)
	go func() {
		for {
			// check task queue
			tasks := task.FilterTaskByStat(wk.Kvops, task.ListTasks(wk.Kvops, wk.Uuid), cf.K_STAT_PEDD)
			if len(tasks) < 1 {
				time.Sleep(2 * time.Second)
				continue
			}
			cf.Log("find launch tasks:", len(tasks))
			// watch key + timeout
			// find new tasks mark.sch tag and assigne to worker
			for _, tsk := range tasks {
				wk.RunTask(tsk)
			}
		}
	}()
}

func (wk *DumyWorker) RunTask(tsk task.Task) {
	// change task stat
	cf.Log("run task:", tsk.Uuid_key)
	task.UpdateStat(wk.Kvops, tsk, cf.K_STAT_STAR, wk.Uuid)
	// 先改一下
	worker_dir, err := os.MkdirTemp("/home/ysj/tmp/", tsk.Uuid_key+".*")
	// worker_dir, err := os.MkdirTemp("/tmp/", tsk.Uuid_key+".*")

	cf.Assert(err == nil, "Create Temp dir fail:%s", err)
	defer os.RemoveAll(worker_dir)
	// get app key and app args
	app_id := wk.Kvops.Get(cf.DotS(tsk.Uuid_key, cf.K_MEMBER_APPUID)).(string)
	exec_app_args := wk.Kvops.Get(cf.DotS(cf.K_AB_CFAPP, app_id, cf.K_MEMBER_APPARGS)).(string)
	exec_file_key := cf.DotS(cf.K_AB_CFAPP, app_id, cf.K_MEMBER_EXEC)
	exec_file_path := path.Join(worker_dir, app_id)
	// download
	fileops := wk.getFileOp(tsk)
	cf.Log("download exec file:", exec_file_key, "to", worker_dir)
	fileops.Get(exec_file_key, exec_file_path)
	os.Chmod(exec_file_path, 0777)
	// run task
	options := []string{} // add default args here
	options = append(options, strings.Split(cf.Base64De(exec_app_args), " ")...)
	cmd := exec.Command(exec_file_path, options...)
	cmd.Env = append(cmd.Env, []string{
		"CF_APP_UUID=" + app_id,
		"CF_APP_HOST=" + wk.Kvops.Host(),
		"CF_APP_PORT=" + cf.Astr(wk.Kvops.Port()),
		"CF_APP_IMP=" + cf.Astr(wk.Kvops.Imp()),
		"CF_APP_SCOPE=" + cf.Astr(wk.Kvops.Scope()),
		"CF_NODE_UUID=" + tsk.Uuid_key,
	}...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err = cmd.Start()
	cf.Assert(err == nil, "run: %s fail: %s", exec_file_path, err)
	cf.Log("worker[", wk.Uuid, "] start:", cmd.String())
	//cmd.Wait()
}

func (wk *DumyWorker) getFileOp(task task.Task) fileops.FileOps {
	app_id := wk.Kvops.Get(cf.DotS(task.Uuid_key, cf.K_MEMBER_APPUID)).(string)
	key := cf.DotS(cf.K_AB_CFAPP, app_id)
	fops, err := wk.FileOps[key]
	if !err {
		fops = fileops.GetFileOps(key, wk.FileCfg)
		wk.FileOps[key] = fops
	} else {
		fops.Conn()
	}
	return fops
}

func NewDumyWorker(kvops kvops.KVOp, filecfg cf.CFG) cfmodule.CfModuleOps {
	worker := DumyWorker{
		FileCfg: filecfg,
		FileOps: map[string]fileops.FileOps{},
	}
	worker.StateCfModule = cfmodule.NewStateCfModule(kvops, "DumyWorker", "a dumy worker")
	return &worker
}
