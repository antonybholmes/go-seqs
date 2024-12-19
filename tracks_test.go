package tracks

import (
	"testing"

	"github.com/antonybholmes/go-dna"
	"github.com/rs/zerolog/log"
)

func TestWithin(t *testing.T) {
	location, err := dna.ParseLocation("chr1:100000-100100")

	if err != nil {
		t.Fatalf(`err %s`, err)
	}

	reader := NewTracksReader("/home/antony/development/data/modules/tracks/chip_seq/hg19/CB4_BCL6_RK040/trackbin/", "max", 100, "hg19")

	reads, err := reader.Reads(location)

	if err != nil {
		t.Fatalf(`err %s`, err)
	}

	log.Debug().Msgf("%v", reads)
}
