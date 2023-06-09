package worker

type WrkOps interface {
	run()
	Sync()
}
