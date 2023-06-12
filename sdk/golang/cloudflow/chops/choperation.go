package chops

type ChannelOp interface {
	Put(ch_name []string, value string) bool
	Watch(ch_name []string, fc func(worker string, subj string, data string) bool) []string
	Close() bool
	CStop(cnkey []string) bool
}
