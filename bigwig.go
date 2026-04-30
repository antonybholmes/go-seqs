package seqs

import (
	"os/exec"
	"strconv"
	"strings"

	"github.com/antonybholmes/go-dna"
)

const (
	BigWigSummaryCmd = "bin/bigWigSummary"
)

func GetBigWigSummary(file string, location dna.Location, bins int) ([]*ReadBin, error) {
	start0 := location.Start() - 1

	cmd := exec.Command(
		BigWigSummaryCmd,
		file,
		location.Chr(),
		strconv.Itoa(start0),
		strconv.Itoa(location.End()),
		strconv.Itoa(bins),
	)

	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	values := strings.Fields(string(out))
	result := make([]*ReadBin, len(values))

	binSize := (location.Len()) / bins

	binStart := location.Start()

	for i, v := range values {
		bin := &ReadBin{
			Start: binStart,
			End:   binStart + binSize - 1,
		}

		binStart += binSize

		if v == "nan" {
			bin.Count = 0
		} else {
			// convert string to float64
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, err
			}
			bin.Count = f
		}

		result[i] = bin
	}

	return result, nil
}
