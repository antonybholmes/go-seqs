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

	track := Track{Platform: "ChIP-seq", Genome: "hg19", Name: "CB4_BCL6_RK040"}

	reader, err := NewTrackReader("/home/antony/development/data/modules/tracks/", track, 100, 0)

	if err != nil {
		t.Fatalf(`err %s`, err)
	}

	binCounts, err := reader.BinCounts(location)

	if err != nil {
		t.Fatalf(`err %s`, err)
	}

	log.Debug().Msgf("%v", binCounts)
}
