package comm

const K_CF_SCHEDUS = "cfschedus" // example: cfschedus.sche.`uuid`.k  scheduler在创建时会作为头部写入etcd中，可以通过该字段是否在etcd中判断scheduler是否存在.
const K_CF_WORKERS = "cfworkers" // worker 在etcd中记录的前缀
const K_CF_APPLIST = "cfapplist" // app 在etch中记录的前缀

// app执行过程中的实例
const K_AB_WORKER = "wokr"  //
const K_AB_SCHEDU = "sche"  //
const K_AB_CFAPP = "cfapp"  //
const K_AB_SERVICE = "srvs" //
const K_AB_SESSION = "sess" //
const K_AB_FLOW = "flow"    //
const K_AB_NODE = "node"    //
const K_AB_TASK = "task"    //

// srvs、task、wokr等的状态
const K_STAT_WAIT = "WAITING"  // 等待
const K_STAT_PEDD = "PENDDING" // 挂起
const K_STAT_WORK = "WORKING"  // 工作中
const K_STAT_STOP = "STOPING"  // 停止中
const K_STAT_EXIT = "EXITED"   // 已关闭
const K_STAT_NONE = "NONE"     // 无状态
const K_STAT_STAR = "STARTING" // 启动中

const K_MEMBER_IS_EXIT = "isexit"
const K_MEMBER_INSCOUNT = "inscount" // 节点实例数量
const K_MEMBER_SUB_INDX = "subidx"   // task的分片标记.如gigasort中sort分36片，对应"qwertyuiopasdfghjklzxcvbnm0987654321"
const K_MEMBER_PARENT = "parent"
const K_MEMBER_EXEC = "exec"
const K_MEMBER_APPUID = "appuid"
const K_MEMBER_APPARGS = "appargs"
const K_MEMBER_RUNCFG = "runcfg"

const K_MESSAGE_EXIT = "EXIT"
const K_MESSAGE_NORM = "NORM"

const CFG_KEY_SRV_MESSAGE = "cf.services.message"
const CFG_KEY_SRV_STATE = "cf.services.state"
const CFG_KEY_SRV_FSTORE = "cf.services.fstore"
const CFG_KEY_SRV_SCEDULER = "cf.scheduler"
const CFG_KEY_SRV_WORKER = "cf.worker"

const NODE_ITYPE_QUEUE = "QUEUE"
const NODE_ITYPE_SUBSC = "SUBSC"
const NODE_ITYPE_INSPC = "INSPERCH"
const NODE_OUYPE_MUT = "MUT"
const NODE_OUYPE_SGL = "SGL"

// storage file stat
const STORAGE_FILE_STAT_ONWRITING = "writing"
const STORAGE_FILE_STAT_ONREADIN = "reading"
const STORAGE_FILE_STAT_FREE = "free"
