# (etcd --data-dir /tmp --listen-client-urls http://127.0.0.1:2379 --advertise-client-urls http://127.0.0.1:2379)&
# (nats-server --js -a 127.0.0.1 -p 4222 -c ../config/service/jetstream.conf)&
# test cluster
(etcd --data-dir /home/ysj/tmp --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://0.0.0.0:2379)&
(nats-server --js -a 0.0.0.0 -p 4222 -c ../config/service/jetstream.conf)&