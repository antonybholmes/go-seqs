package seqdb

import (
	"sync"

	"github.com/antonybholmes/go-seqs"
)

var instance *seqs.SeqDB
var once sync.Once

func InitSeqDB(dir string) *seqs.SeqDB {
	once.Do(func() {
		instance = seqs.NewSeqDB(dir)
	})

	return instance
}

func GetInstance() *seqs.SeqDB {
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

func Tracks(platform string, genome string) ([]seqs.Sample, error) {
	return instance.Seqs(platform, genome)
}

func Search(genome string, query string) ([]seqs.Sample, error) {
	return instance.Search(genome, query)
}

func ReaderFromId(publicId string, binWidth int, scale float64) (*seqs.SeqReader, error) {
	return instance.ReaderFromId(publicId, binWidth, scale)
}

func HasPermissionToViewDataset(datasetId string, permissions []string) error {
	return instance.HasPermissionToViewDataset(datasetId, permissions)
}
