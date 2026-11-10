package seqdb

import (
	"sync"

	"github.com/antonybholmes/go-seqs"
)

var instance *seqs.SeqDB
var once sync.Once

func InitSeqDB(path string) *seqs.SeqDB {
	once.Do(func() {
		instance = seqs.NewSeqDB(path)
	})

	return instance
}

func Instance() *seqs.SeqDB {
	return instance
}
