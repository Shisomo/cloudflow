
if [ ! -f bin/etcd ]; then
    echo "download etcd"
    etcd_ver="v3.5.9"
    etcd_arc="etcd-${etcd_ver}-linux-amd64"
    etcd_tar="${etcd_arc}.tar.gz"
    wget -P /tmp https://github.com/etcd-io/etcd/releases/download/${etcd_ver}/${etcd_tar}
    tar xf /tmp/${etcd_tar} -C /tmp/
    mv /tmp/${etcd_arc}/etcd bin/
    rm -rf /tmp/${etcd_arc}*
fi

if [ -f bin/etcd ]; then
    bin/etcd $@
else
    echo "download etcd fail"
fi
