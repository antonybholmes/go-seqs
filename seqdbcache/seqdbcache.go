package seqdbcache

import (
	"sync"

	"github.com/antonybholmes/go-seq"
)

var instance *seq.SeqDB
var once sync.Once

func InitCache(dir string) *seq.SeqDB {
	once.Do(func() {
		instance = seq.NewSeqDB(dir)
	})

	return instance
}

func GetInstance() *seq.SeqDB {
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

func Tracks(platform string, genome string) ([]seq.SeqInfo, error) {
	return instance.Tracks(platform, genome)
}

func Search(genome string, query string) ([]seq.SeqInfo, error) {
	return instance.Search(genome, query)
}

func ReaderFromId(publicId string, binWidth uint, scale float64) (*seq.SeqReader, error) {
	return instance.ReaderFromId(publicId, binWidth, scale)
}
