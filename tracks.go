package tracks

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/antonybholmes/go-dna"
	"github.com/rs/zerolog/log"
)

const MAGIC_NUMBER_OFFSET_BYTES = 0
const BIN_SIZE_OFFSET_BYTES = MAGIC_NUMBER_OFFSET_BYTES + 4
const BIN_WIDTH_OFFSET_BYTES = BIN_SIZE_OFFSET_BYTES + 1
const N_BINS_OFFSET_BYTES = BIN_WIDTH_OFFSET_BYTES + 4
const BINS_OFFSET_BYTES = N_BINS_OFFSET_BYTES + 4

type BinCounts struct {
	Location *dna.Location `json:"location"`
	Start    uint          `json:"start"`
	Reads    []uint32      `json:"reads"`
	ReadN    uint32        `json:"readn"`
}

type TracksReader struct {
	Dir      string
	Mode     string
	BinWidth uint
	ReadN    uint32
	Genome   string
}

func NewTracksReader(dir string, mode string, binWidth uint, genome string) *TracksReader {

	file, err := os.Open(filepath.Join(dir, "reads.txt"))
	if err != nil {

		log.Fatal().Msgf("error opening %s", dir)
	}

	defer file.Close()
	// Create a scanner
	scanner := bufio.NewScanner(file)
	scanner.Scan()

	count, err := strconv.Atoi(scanner.Text())
	if err != nil {
		log.Fatal().Msgf("could not count reads")
	}

	return &TracksReader{Dir: dir,
		Mode:     mode,
		BinWidth: binWidth,
		ReadN:    uint32(count),
		Genome:   genome}
}

func (reader *TracksReader) getPath(location *dna.Location) string {
	return filepath.Join(reader.Dir, fmt.Sprintf("%s_bw%d_c%s_%s.trackbin", strings.ToLower(location.Chr), reader.BinWidth, reader.Mode, reader.Genome))

}

func (reader *TracksReader) Reads1Byte(location *dna.Location) (*BinCounts, error) {
	file := reader.getPath(location)

	f, err := os.Open(file)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	var magic uint32
	binary.Read(f, binary.LittleEndian, &magic)
	var binSizeBytes byte
	binary.Read(f, binary.LittleEndian, &binSizeBytes)

	switch binSizeBytes {
	case 1:
		return reader.ReadsUint8(location)
	case 2:
		return reader.ReadsUint16(location)
	default:
		return reader.ReadsUint32(location)
	}
}

func (reader *TracksReader) ReadsUint8(location *dna.Location) (*BinCounts, error) {
	s := location.Start - 1
	e := location.End - 1

	bs := s / reader.BinWidth
	be := e / reader.BinWidth
	bl := be - bs + 1

	file := reader.getPath(location)

	f, err := os.Open(file)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	//var magic uint32
	//binary.Read(f, binary.LittleEndian, &magic)

	f.Seek(9, 0)

	var binCount uint32
	binary.Read(f, binary.LittleEndian, &binCount)

	log.Debug().Msgf("bins %d", binCount)

	data := make([]uint8, bl)
	f.Seek(int64(BINS_OFFSET_BYTES+bs), 0)
	binary.Read(f, binary.LittleEndian, &data)

	reads := make([]uint32, bl)

	for i, c := range data {
		reads[i] = uint32(c)
	}

	return reader.Results(location, reads)
}

func (reader *TracksReader) ReadsUint16(location *dna.Location) (*BinCounts, error) {
	s := location.Start - 1
	e := location.End - 1

	bs := s / reader.BinWidth
	be := e / reader.BinWidth
	bl := be - bs + 1

	file := reader.getPath(location)

	f, err := os.Open(file)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	f.Seek(9, 0)

	var binCount uint32
	binary.Read(f, binary.LittleEndian, &binCount)

	data := make([]uint16, bl)
	f.Seek(int64(BINS_OFFSET_BYTES+bs), 0)
	binary.Read(f, binary.LittleEndian, &data)

	reads := make([]uint32, bl)

	for i, c := range data {
		reads[i] = uint32(c)
	}

	return reader.Results(location, reads)
}

func (reader *TracksReader) ReadsUint32(location *dna.Location) (*BinCounts, error) {
	s := location.Start - 1
	e := location.End - 1

	bs := s / reader.BinWidth
	be := e / reader.BinWidth
	bl := be - bs + 1

	file := reader.getPath(location)

	f, err := os.Open(file)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	f.Seek(9, 0)

	var binCount uint32
	binary.Read(f, binary.LittleEndian, &binCount)

	reads := make([]uint32, bl)
	f.Seek(int64(BINS_OFFSET_BYTES+bs), 0)
	binary.Read(f, binary.LittleEndian, &reads)

	return reader.Results(location, reads)
}

func (reader *TracksReader) Results(location *dna.Location, reads []uint32) (*BinCounts, error) {
	s := location.Start - 1

	bs := s / reader.BinWidth

	return &BinCounts{
		Location: location,
		Start:    bs*reader.BinWidth + 1,
		Reads:    reads,
		ReadN:    reader.ReadN,
	}, nil
}
