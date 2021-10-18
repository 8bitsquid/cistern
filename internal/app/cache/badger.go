package cache

import (
	"io"

	"github.com/dgraph-io/badger"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/internal/pkg/cache"
)

type Badger struct {
	db *badger.DB
	basePath string
}

func (b *Badger) BasePath() string {
	panic("not implemented") // TODO: Implement
}

func (b *Badger) Exists(key string) bool {
	panic("not implemented") // TODO: Implement
}

func (b *Badger) Get(key string) (cache.Item, error) {
	panic("not implemented") // TODO: Implement
}

func (b *Badger) FindAll(key string) ([]string, error) {
	panic("not implemented") // TODO: Implement
}

func (b *Badger) Cache(key string, val []byte) error {
	panic("not implemented") // TODO: Implement
}

func (b *Badger) CacheFromReader(key string, r io.Reader) error {
	panic("not implemented") // TODO: Implement
}

func (b *Badger) Delete(key string) error {
	panic("not implemented") // TODO: Implement
}

func (b *Badger) DeleteAllWithPath(path string) error {
	panic("not implemented") // TODO: Implement
}

func (b *Badger) Flush() ([]cache.Item, error) {
	panic("not implemented") // TODO: Implement
}

func (b *Badger) FlushWithPath(path string) ([]cache.Item, error) {
	panic("not implemented") // TODO: Implement
}

