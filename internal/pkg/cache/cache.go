package cache

import (
	"io"
)

// ICache ...
type Cache interface {
	BasePath() string
	Exists(key string) bool
	Get(key string) (Item, error)
	FindAll(key string) ([]string, error)
	Cache(key string, val []byte) error
	CacheFromReader(key string, r io.Reader) error
	Delete(key string) error
	DeleteAllWithPath(path string) error
	Flush() ([]Item, error)
	FlushWithPath(path string) ([]Item, error)

}

type Item interface {
	Key() string
	Value() []byte
	Size() int64
	ExpiresAt() uint64 // time.Unix
}

type CacheOptions struct {
	Namespace string
	ExpiresAt uint64
}

type CacheOption func(*CacheOptions)

func WithNamespace(ns string) CacheOption {
	return func(o *CacheOptions) {
		o.Namespace = ns
	}
}

func ExpiresAt(ts uint64) CacheOption {
	return func(o *CacheOptions) {
		o.ExpiresAt = ts
	}
}