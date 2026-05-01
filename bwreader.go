package seqs

import (
	"os/exec"
	"strconv"
	"strings"

	basemath "github.com/antonybholmes/go-basemath"
	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-sys/log"
)

const (
	BigWigSummaryCmd = "bin/bigWigSummary"
)

type BigWigSeqReader struct {
	sample  *Sample
	url     string
	binSize int
}

func NewBigWigReader(sample *Sample, binSize int) (SeqReader, error) {

	return &BigWigSeqReader{
		sample:  sample,
		url:     sample.Url,
		binSize: binSize,

		// estimate the number of bins to represent a region
		//defaultBinCount: binSize * 4,
	}, nil
}

// func (reader *SeqReader) getPath(location *dna.Location) string {
// 	return filepath.Join(reader.Dir, fmt.Sprintf("bin%d", reader.BinSize), fmt.Sprintf("%s_bin%d_%s.db?mode=ro", location.Chr, reader.BinSize, reader.Track.Genome))
// }

func (reader *BigWigSeqReader) BinCounts(location *dna.Location) (*SampleBinCounts, error) {

	//var startBin uint = (location.Start - 1) / reader.BinSize
	//var endBin uint = (location.End - 1) / reader.BinSize

	log.Debug().Msgf("getting bigwig summary for location %s with bin size %d and url %s", location, reader.binSize, reader.url)

	// we return something for every call, even if data not available
	ret := SampleBinCounts{
		Id: reader.sample.Id,
		//Name: reader.sample.Name,
		//Track:    reader.Track,
		//Location: location,
		//Start:    startBin*reader.BinSize + 1,
		//Chr:     location.Chr,
		Bins:     make([]*ReadBin, 0, reader.binSize),
		YMax:     0,
		BinSize:  reader.binSize,
		Reads:    -1,
		BinReads: -1,
	}

	loc, err := alignLocToBinSize(location, reader.binSize)
	if err != nil {
		return &ret, err
	}

	readBins, err := GetBigWigSummary(reader.url, loc, reader.binSize)

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

func GetBigWigSummary(url string, location *dna.Location, binSize int) ([]*ReadBin, error) {
	start0 := location.Start() - 1

	// bigWigSummary returns a number of bins, so we must calculate the number of bins to return based on the location length and bin size
	bins := location.Len() / binSize

	log.Debug().Msgf("getting bigwig summary for location %s with bin size %d and url %s, calculated bins %d", location, binSize, url, bins)

	cmd := exec.Command(
		BigWigSummaryCmd,
		url,
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
	start := location.Start()
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
