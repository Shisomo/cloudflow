
echo "$0 build cloudflow.src ..."
CGO_ENABLED=0 go build --ldflags "-extldflags -static" -o bin/cf
if [ $? == 0 ]; then
  echo "$0 run cf"
  bin/cf $@
fi