

#### cloudflow 状态布局 （KV）

##### 1.管理（常驻）
```
# 应用列表
scope.cfapplist:       [app_id_1, app_id_2, app_id_3, app_id_4, ...]
scope.cfapplist.ctime: time       # 创建时间

# 通用项: 每一类数据都有
scope.cfapplist.descr: description # 描述
scope.cfapplist.cstat: ALIVE      # 通用状态
scope.cfapplist.stags: ALIVE      # 静态标签
scope.cfapplist.atags: ALIVE      # 动态状态
scope.cfapplist.count: 1000       # 调用次数
scope.cfapplist.dsize: 1000       # 调用数据量(字节)
scope.cfapplist.atime: time       # 最后修改时间
scope.cfapplist.whoac: node_id    # 最后修改者
scope.cfapplist.oplog: txtlog     # 操作日志
scope.cfapplist.lock.owner:       # 更新锁.创建者
scope.cfapplist.lock.ctime:       # 更新锁.创建时间
scope.cfapplist.lock.ltime:       # 更新锁.预计耗时(s)

# worker列表
scope.cfworkers:       [worker_id_1, worker_id_2, worker_id_3, ...]
scope.cfworkers.ctime: time       # 创建时间

# 调度器列表
scope.cfschedulers:       [scheduler_id_1, scheduler_id_2, ...]
scope.cfschedulers.ctime: time      # 创建时间 
```

##### 2.应用
###### 2.1 基本信息
```
scope.cfapp.xxxx.ctime:
scope.cfapp.xxxx.name:
scope.cfapp.xxxx.rawcfg:
scope.cfapp.xxxx.sdkv:
scope.cfapp.xxxx.srvs:   [id1, id2, ...]
scope.cfapp.xxxx.sess:   [id1, id2, ...]
scope.cfapp.xxxx.comm*
```

###### 2.2 rpc服务
```
scope.srvs.xxxxx.atime:
scope.srvs.xxxxx.ctime:
scope.srvs.xxxxx.func:
scope.srvs.xxxxx.index:
scope.srvs.xxxxx.inscount:
scope.srvs.xxxxx.name:
scope.srvs.xxxxx.subidx:
scope.srvs.xxxxx.uuid:
scope.srvs.xxxxx.comm*
```

###### 2.3 会话
```
scope.sess.xxxxx.name:
scope.sess.xxxxx.index:
scope.sess.xxxxx.uuid:
scope.sess.xxxxx.ctime:
scope.sess.xxxxx.flows: [id1, id2, ...]
scope.sess.xxxxx.comm*
```

###### 2.4 处理流
```
scope.flow.xxxxx.ctime:
scope.flow.xxxxx.uuid:
scope.flow.xxxxx.index:
scope.flow.xxxxx.name:
scope.flow.xxxxx.nodes: [id1, id2, ...]
scope.flow.xxxxx.comm*
```


###### 2.5 执行节点
```
scope.node.xxxxx.uuid:
scope.node.xxxxx.ctime:
scope.node.xxxxx.func:
scope.node.xxxxx.index:
scope.node.xxxxx.inscount:
scope.node.xxxxx.subidx:
scope.node.xxxxx.synchz:
scope.node.xxxxx.state:
scope.node.xxxxx.comm*
```


##### 3.调度与worker
###### 3.1 worker
```
scope.wokr.xxxxx.uuid:
scope.wokr.xxxxx.name:
scope.wokr.xxxxx.task:   [node1, node2, ...]
scope.wokr.xxxxx.ctime:
scope.wokr.xxxxx.*       # worker自身相关
```

###### 3.2 调度器
```
scope.sche.xxxxx.uuid:
scope.sche.xxxxx.name:
scope.sche.xxxxx.ctime:
scope.sche.xxxxx.*       # worker自身相关
```
