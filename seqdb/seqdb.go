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

// func Genomes(permissions []string) ([]string, error) {
// 	return instance.Genomes(permissions)
// }

func Platforms(assembly string, isAdmin bool, permissions []string) ([]*seqs.Platform, error) {
	return instance.Platforms(assembly, isAdmin, permissions)
}

func Datasets(assembly string, isAdmin bool, permissions []string) ([]*seqs.Dataset, error) {
	return instance.Datasets(assembly, isAdmin, permissions)
}

func PlatformDatasets(platform string, assembly string, isAdmin bool, permissions []string) ([]*seqs.Dataset, error) {
	return instance.PlatformDatasets(platform, assembly, isAdmin, permissions)
}

func SearchSamples(query string, assembly string, isAdmin bool, permissions []string) ([]*seqs.Sample, error) {
	return instance.Search(query, assembly, isAdmin, permissions)
}

func ReaderFromId(publicId string, binWidth int, scale float64) (*seqs.SeqReader, error) {
	return instance.ReaderFromId(publicId, binWidth, scale)
}

func CanViewSample(sampleId string, isAdmin bool, permissions []string) error {
	return instance.CanViewSample(sampleId, isAdmin, permissions)
}
