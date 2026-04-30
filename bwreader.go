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

func NewBigWigReader(sample *Sample, url string, binSize int) (SeqReader, error) {

	return &BigWigSeqReader{
		sample:  sample,
		url:     url,
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

	readBins, err := GetBigWigSummary(reader.url, location, reader.binSize)

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

func GetBigWigSummary(url string, location *dna.Location, bins int) ([]*ReadBin, error) {
	start0 := location.Start() - 1

	log.Debug().Msgf("running bigwig summary %s %s:%d-%d bins %d", url, location.Chr(), location.Start(), location.End(), bins)

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
