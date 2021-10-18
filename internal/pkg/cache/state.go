package cache



type IState interface {
	GetState(key string) ([]byte, error)
	SetState(key string, val []byte) error
	GetState(key string) []byte
	SetState(key string, data []byte)
	SetStateWithName(key string, data []byte)
}