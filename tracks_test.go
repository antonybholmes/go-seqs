package tracks

import (
	"testing"

	"github.com/antonybholmes/go-dna"
	"github.com/rs/zerolog/log"
)

func TestWithin(t *testing.T) {
	location, err := dna.ParseLocation("chr1:99000-100100")

	if err != nil {
		t.Fatalf(`err %s`, err)
	}

	reader := NewTracksReader("/home/antony/development/data/modules/tracks/chip_seq/hg19/CB4_BCL6_RK040/", "max", 100, "hg19")

	binCounts, err := reader.BinCounts(location)

	if err != nil {
		t.Fatalf(`err %s`, err)
	}

	log.Debug().Msgf("%v", binCounts)
}
