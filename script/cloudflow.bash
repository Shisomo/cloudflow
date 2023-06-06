
CGO_ENABLED=0 go build --ldflags "-extldflags -static" -o bin/cf

if [ $? == 0 ]; then
  bin/cf $@
fi