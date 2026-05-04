package seqs

import (
	"os/exec"
	"strconv"
	"strings"

	basemath "github.com/antonybholmes/go-basemath"
	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-sys/log"
)

type BigWigSeqReader struct {
	sample  *Sample
	url     string
	binSize int
}

const (
	BigWigSummaryCmd = "bin/bigWigSummary"
)

func NewBigWigReader(sample *Sample, binSize int) (SeqReader, error) {

	return &BigWigSeqReader{
		sample:  sample,
		url:     sample.Url,
		binSize: binSize,

		// estimate the number of bins to represent a region
		//defaultBinCount: binSize * 4,
	}, nil
}

func (reader *BigWigSeqReader) BinCounts(location *dna.Location) (*SampleBinCounts, error) {

	log.Debug().Msgf("getting bigwig summary for location %s with bin size %d and url %s", location, reader.binSize, reader.url)

	// we return something for every call, even if data not available
	ret := SampleBinCounts{
		Id:      reader.sample.Id,
		Bins:    make([]*ReadBin, 0, reader.binSize),
		YMax:    0,
		BinSize: reader.binSize,
	}

	readBins, err := getBigWigSummary(reader.url, location, reader.binSize)

	if err != nil {
		log.Debug().Msgf("error reading bigwig summary %s %s", reader.url, err)
		return &ret, err
	}

	ret.Bins = readBins

	for _, bin := range ret.Bins {
		ret.YMax = basemath.Max(ret.YMax, bin.Count)
	}

	return &ret, nil
}

// Return a location aligned to the bin size, so that the start is a multiple of the bin size
// and the end is a multiple of the bin size. This is necessary because bigwig summary
// requires locations to be aligned to the bin size.
func alignLocToBinSize(location *dna.Location, binSize int) (*dna.Location, error) {
	start := (location.Start())/binSize*binSize + 1

	// align end to be a multiple of bin size
	end := ((location.End()-1)/binSize + 1) * binSize

	loc, err := dna.NewLocation(location.Chr(), start, end)

	if err != nil {
		log.Debug().Msgf("error aligning location to bin size: %s", err)
		return nil, err
	}

	return loc, nil
}

func getBigWigSummary(url string, location *dna.Location, binSize int) ([]*ReadBin, error) {
	// ensure aligned to bin size by aligning start and end to the nearest multiple of bin size
	// for example, if bin size is 1000, and location is chr1:1500-2500, we would align to chr1:1000-3000
	// if location is chr1:500-1500, we would align to chr1:0-2000
	locBinSizeAligned, err := alignLocToBinSize(location, binSize)

	if err != nil {
		return nil, err
	}

	//log.Debug().Msgf("location %s aligned to %s", location, locBinSizeAligned)

	start0 := locBinSizeAligned.Start() - 1

	// bigWigSummary returns a number of bins, so we must calculate the number of bins to return based on the location length and bin size
	bins := locBinSizeAligned.Len() / binSize

	//log.Debug().Msgf("getting bigwig summary for location %s with bin size %d and url %s, calculated bins %d", locBinSizeAligned, binSize, url, bins)

	cmd := exec.Command(
		BigWigSummaryCmd,
		url,
		locBinSizeAligned.Chr(),
		strconv.Itoa(start0),
		strconv.Itoa(locBinSizeAligned.End()),
		strconv.Itoa(bins),
	)

	out, err := cmd.Output()

	if err != nil {
		return nil, err
	}

	values := strings.Fields(string(out))
	result := make([]*ReadBin, len(values))
	start := locBinSizeAligned.Start()
	binEndWidth := binSize - 1

	for i, v := range values {
		bin := &ReadBin{
			Start: start,
			End:   start + binEndWidth,
		}

		start += binSize

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
