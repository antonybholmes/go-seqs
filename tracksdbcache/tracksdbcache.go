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

func Genomes() ([]string, error) {
	return instance.Genomes()
}

func Platforms(genome string) ([]string, error) {
	return instance.Platforms(genome)
}

func Tracks(platform string, genome string) ([]tracks.TrackInfo, error) {
	return instance.Tracks(platform, genome)
}

func Search(genome string, query string) ([]tracks.TrackInfo, error) {
	return instance.Search(genome, query)
}

func ReaderFromId(publicId string, binWidth uint, scale float64) (*tracks.TrackReader, error) {
	return instance.ReaderFromId(publicId, binWidth, scale)
}
