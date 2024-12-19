package tracks

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/antonybholmes/go-dna"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

const MAGIC_NUMBER_OFFSET_BYTES = 0
const BIN_SIZE_OFFSET_BYTES = MAGIC_NUMBER_OFFSET_BYTES + 4
const BIN_WIDTH_OFFSET_BYTES = BIN_SIZE_OFFSET_BYTES + 4
const N_BINS_OFFSET_BYTES = BIN_WIDTH_OFFSET_BYTES + 4
const BINS_OFFSET_BYTES = N_BINS_OFFSET_BYTES + 4

const BIN_SQL = `SELECT bin, reads 
	FROM track
 	WHERE bin >= ?1 AND bin <= ?2
	ORDER BY bin`

type BinCounts struct {
	Location *dna.Location `json:"location"`
	Reads    []uint32      `json:"reads"`
	Start    uint          `json:"start"`
	ReadN    uint          `json:"readn"`
}

type TracksReader struct {
	Dir      string
	Mode     string
	Genome   string
	BinWidth uint
	ReadN    uint
}

func NewTracksReader(dir string, mode string, binWidth uint, genome string) *TracksReader {

	file, err := os.Open(filepath.Join(dir, fmt.Sprintf("reads_%s.txt", genome)))
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
		ReadN:    uint(count),
		Genome:   genome}
}

func (reader *TracksReader) getPath(location *dna.Location) string {
	return filepath.Join(reader.Dir, fmt.Sprintf("%s_bw%d_c%s_%s.db", strings.ToLower(location.Chr), reader.BinWidth, reader.Mode, reader.Genome))

}

func (reader *TracksReader) Reads(location *dna.Location) (*BinCounts, error) {

	path := reader.getPath(location)

	db, err := sql.Open("sqlite3", path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	s := location.Start - 1
	e := location.End - 1

	startBin := s / reader.BinWidth
	endBin := e / reader.BinWidth

	rows, err := db.Query(BIN_SQL,
		startBin,
		endBin)

	if err != nil {
		return nil, err
	}

	var bin uint32
	var count uint32
	reads := make([]uint32, endBin-startBin+1)
	index := 0
	for rows.Next() {
		err := rows.Scan(&bin, &count)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		reads[index] = count
		index++
	}

	return &BinCounts{
		Location: location,
		Start:    startBin*reader.BinWidth + 1,
		Reads:    reads,
		ReadN:    reader.ReadN,
	}, nil

	// var magic uint32
	// binary.Read(f, binary.LittleEndian, &magic)
	// var binSizeBytes byte
	// binary.Read(f, binary.LittleEndian, &binSizeBytes)

	// switch binSizeBytes {
	// case 1:
	// 	return reader.ReadsUint8(location)
	// case 2:
	// 	return reader.ReadsUint16(location)
	// default:
	// 	return reader.ReadsUint32(location)
	// }
}

// func (reader *TracksReader) ReadsUint8(location *dna.Location) (*BinCounts, error) {
// 	s := location.Start - 1
// 	e := location.End - 1

// 	bs := s / reader.BinWidth
// 	be := e / reader.BinWidth
// 	bl := be - bs + 1

// 	file := reader.getPath(location)

// 	f, err := os.Open(file)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer f.Close()

// 	//var magic uint32
// 	//binary.Read(f, binary.LittleEndian, &magic)

// 	f.Seek(9, 0)

// 	offset := BINS_OFFSET_BYTES + bs
// 	log.Debug().Msgf("offset %d %d", offset, bs)

// 	data := make([]uint8, bl)
// 	f.Seek(int64(offset), 0)
// 	binary.Read(f, binary.LittleEndian, &data)

// 	reads := make([]uint32, bl)

// 	for i, c := range data {
// 		reads[i] = uint32(c)
// 	}

// 	return reader.Results(location, bs, reads)
// }

// func (reader *TracksReader) ReadsUint16(location *dna.Location) (*BinCounts, error) {
// 	s := location.Start - 1
// 	e := location.End - 1

// 	bs := s / reader.BinWidth
// 	be := e / reader.BinWidth
// 	bl := be - bs + 1

// 	file := reader.getPath(location)

// 	f, err := os.Open(file)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer f.Close()

// 	f.Seek(9, 0)

// 	data := make([]uint16, bl)
// 	f.Seek(int64(BINS_OFFSET_BYTES+bs*2), 0)
// 	binary.Read(f, binary.LittleEndian, &data)

// 	reads := make([]uint32, bl)

// 	for i, c := range data {
// 		reads[i] = uint32(c)
// 	}

// 	return reader.Results(location, bs, reads)
// }

// func (reader *TracksReader) ReadsUint32(location *dna.Location) (*BinCounts, error) {
// 	s := location.Start - 1
// 	e := location.End - 1

// 	bs := s / reader.BinWidth
// 	be := e / reader.BinWidth
// 	bl := be - bs + 1

// 	file := reader.getPath(location)

// 	f, err := os.Open(file)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer f.Close()

// 	f.Seek(9, 0)

// 	reads := make([]uint32, bl)
// 	f.Seek(int64(BINS_OFFSET_BYTES+bs*4), 0)
// 	binary.Read(f, binary.LittleEndian, &reads)

// 	return reader.Results(location, bs, reads)
// }

// func (reader *TracksReader) Results(location *dna.Location, bs uint, reads []uint32) (*BinCounts, error) {

// 	return &BinCounts{
// 		Location: location,
// 		Start:    bs*reader.BinWidth + 1,
// 		Reads:    reads,
// 		ReadN:    reader.ReadN,
// 	}, nil
// }
