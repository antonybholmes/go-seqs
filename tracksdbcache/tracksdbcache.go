package tracksdbcache

import (
	"sync"

	"github.com/antonybholmes/go-tracks"
)

var instance *tracks.TracksDB
var once sync.Once

func InitCache(dir string) *tracks.TracksDB {
	once.Do(func() {
		instance = tracks.NewTrackDB(dir)
	})

	return instance
}

func GetInstance() *tracks.TracksDB {
	return instance
}

func Dir() string {
	return instance.Dir()
}
