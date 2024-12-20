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

func Platforms() []string {
	return instance.Platforms()
}

func Genomes(platform string) ([]string, error) {
	return instance.Genomes(platform)
}

func Tracks(platform string, genome string) ([]tracks.TrackInfo, error) {
	return instance.Tracks(platform, genome)
}

func AllTracks() (*tracks.AllTracks, error) {
	return instance.AllTracks()
}

func Reader(track tracks.Track, binWidth uint) (*tracks.TrackReader, error) {
	return tracks.NewTrackReader(instance.Dir(), track, binWidth, "mean")
}
