package comm

type kvLock struct {
	Owner string `json:"owner"` // 创建者
	Ctime int64  `json:"ctime"` // 创建时间
	Ltime int64  `json:"ltime"` // 预计耗时(s)
}

type CommStat struct {
	Parent string `json:"parent"`
	Option string `json:"optin"` // 选项
	Descr  string `json:"descr"` // 描述
	Cstat  string `json:"cstat"` // 通用状态
	STags  string `json:"stags"` // 静态标签
	Atags  string `json:"atags"` // 动态标签
	Count  int64  `json:"count"` // 活动次数
	DSize  int64  `json:"dsize"` // 数据量(字节)
	Atime  int64  `json:"atime"` // 最后修改时间
	WhoAc  string `json:"whoac"` // 修改者
	OpLog  string `json:"oplog"` // 操作日志
	Lock   kvLock `json:"lock"`  // 修改锁
	Host   string `json:"host"`  // Host唯一标识
}

const CSTAT_EMPTY = "EMPTY"     // 空状态，默认
const CSTAT_ACTIVE = "ACTIVE"   // 活动
const CSTAT_DEAD = "DEAD"       // 死亡
const CSTAT_PENDING = "PENDING" // 挂起
const CSTAT_STOP = "STOP"       // 停止
const CSTAT_DEL = "DEL"         // 删除，状态数据由调度器、work等服务进行删除（不能由用户应用层删除）
