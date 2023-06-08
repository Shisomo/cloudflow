

#### cloudflow 状态布局 （KV）

##### 1.管理
```
# 应用列表
scope.cfapplist:       [app_id_1, app_id_2, app_id_3, app_id_4, ...]
scope.cfapplist.atime: time       # 最后修改时间
scope.cfapplist.whoac: node_id    # 最后修改者

# worker列表
scope.cfworkers:       [worker_id_1, worker_id_2, worker_id_3, ...]
scope.cfworkers.atime: time       # 最后修改时间
scope.cfworkers.whoac: node_id    # 最后修改者

# 调度器列表
scope.cfschedulers:       [scheduler_id_1, scheduler_id_2, ...]
scope.cfschedulers.atime: time      # 最后修改时间 
scope.cfschedulers.whoac: node_id   # 最后修改者
```

##### 2.应用
###### 2.1 基本信息
```
scope.cfapp.xxxx.state:   RUN  # 
scope.cfapp.xxxx.ctime:
scope.cfapp.xxxx.name:
scope.cfapp.xxxx.rawcfg:
scope.cfapp.xxxx.sdkv:
scope.cfapp.xxxx.srvs:   [id1, id2, ...]
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
```

###### 2.3 会话
```
scope.sess.xxxxx.name:
scope.sess.xxxxx.index:
scope.sess.xxxxx.uuid:
scope.sess.xxxxx.ctime:
scope.sess.xxxxx.flows: [id1, id2, ...]
```

###### 2.4 处理流
```
scope.flow.xxxxx.ctime:
scope.flow.xxxxx.uuid:
scope.flow.xxxxx.index:
scope.flow.xxxxx.name:
scope.flow.xxxxx.nodes: [id1, id2, ...]
```


###### 2.5 执行节点
```
scope.node.xxxxx.uuid:
scope.node.xxxxx.atime:
scope.node.xxxxx.ctime:
scope.node.xxxxx.func:
scope.node.xxxxx.index:
scope.node.xxxxx.inscount:
scope.node.xxxxx.subidx:
scope.node.xxxxx.synchz:
scope.node.xxxxx.state:
```
