(etcd --data-dir /tmp --listen-client-urls http://127.0.0.1:2379 --advertise-client-urls http://127.0.0.1:2379)&
(nats-server --js -a 127.0.0.1 -p 4222)&