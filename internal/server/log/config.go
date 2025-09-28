package log

type Config struct {
	Segment struct {
		MaxIndexBytes uint64
		MaxStoreBytes uint64
		InitialOffset uint64
	}
}
