package seqs

import (
	"testing"

	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-sys/log"
)

func TestWithin(t *testing.T) {
	location, err := dna.ParseLocation("chr1:99000-100100")

	if err != nil {
		t.Fatalf(`err %s`, err)
	}

	track := "00000000-0000-0000-0000-000000000001"

	reader, err := NewSeqReader("/home/antony/development/data/modules/seqs/", track, 100, 0)

	if err != nil {
		t.Fatalf(`err %s`, err)
	}

	binCounts, err := reader.SampleBinCounts(location)

	if err != nil {
		t.Fatalf(`err %s`, err)
	}

	log.Debug().Msgf("%v", binCounts)
}
