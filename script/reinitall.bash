# need first:
#  /usr/bin/etcd --data-dir /tmp --listen-client-urls http://127.0.0.1:2379 --advertise-client-urls http://127.0.0.1:2379
#  /usr/local/bin/nats-server --js -a 127.0.0.1 -p 4222

# kill worker and scheduler
kill -9 `pidof cf`

# clear all ObjectStorage
bucket=`nats object ls|awk '{print $2}'|grep -v "^$"|grep -v "Object"|grep -v "Bucket"|xargs`
for v in ${bucket}; do
  nats object del $v -f
done

# clear all etcd
ETCDCTL_API=3 etcdctl del "" --prefix

# start worker and  
#(bash script/cloudflow.bash worker 1>log/worker.log 2>log/worker.err)&
(bash script/cloudflow.bash sc 1>log/scheduler.log 2>log/scheduler.err)&

echo "stat:"
ETCDCTL_API=3 etcdctl get "" --prefix

echo "file:"
nats object ls
