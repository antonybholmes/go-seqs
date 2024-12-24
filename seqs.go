package seqs

import (
	"database/sql"
	"fmt"
	"path/filepath"

	basemath "github.com/antonybholmes/go-basemath"
	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-sys"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

// const MAGIC_NUMBER_OFFSET_BYTES = 0
// const BIN_SIZE_OFFSET_BYTES = MAGIC_NUMBER_OFFSET_BYTES + 4
// const BIN_WIDTH_OFFSET_BYTES = BIN_SIZE_OFFSET_BYTES + 4
// const N_BINS_OFFSET_BYTES = BIN_WIDTH_OFFSET_BYTES + 4
// const BINS_OFFSET_BYTES = N_BINS_OFFSET_BYTES + 4

const GENOMES_SQL = `SELECT DISTINCT genome FROM tracks ORDER BY genome`
const PLATFORMS_SQL = `SELECT DISTINCT platform FROM tracks WHERE genome = ?1 ORDER BY platform`

const TRACK_SQL = `SELECT public_id, name, reads, stat_mode FROM track`

const TRACKS_SQL = `SELECT id, public_id, genome, platform, name, reads, stat_mode, dir 
	FROM tracks 
	WHERE genome = ?1 AND platform = ?2 
	ORDER BY name`

const ALL_SEQS_SQL = `SELECT id, public_id, genome, platform, name, reads, stat_mode, dir 
	FROM tracks 
	WHERE genome = ?1 
	ORDER BY genome, platform, name`

const SEQ_FROM_ID_SQL = `SELECT id, public_id, genome, platform, name, reads, stat_mode, dir 
	FROM tracks 
	WHERE public_id = ?1`

const SEARCH_SEQS_SQL = `SELECT id, public_id, genome, platform, name, reads, stat_mode, dir 
	FROM tracks 
	WHERE genome = ?1 AND (public_id = ?1 OR platform = ?1 OR name LIKE ?2)
	ORDER BY genome, platform, name`

const BIN_SQL = `SELECT start, end, reads 
	FROM bins
 	WHERE start >= ?1 AND end < ?2
	ORDER BY start`

type BinCounts struct {
	Track    Track         `json:"track"`
	Location *dna.Location `json:"location"`
	Bins     []uint        `json:"bins"`
	YMax     uint          `json:"ymax"`
	Start    uint          `json:"start"`
	BinWidth uint          `json:"binWidth"`
}

type Track struct {
	Genome   string `json:"genome"`
	Platform string `json:"platform"`
	Name     string `json:"name"`
}

type SeqInfo struct {
	PublicId string `json:"publicId"`
	Genome   string `json:"genome"`
	Platform string `json:"platform"`
	Name     string `json:"name"`
	Stat     string `json:"stat"`
	Reads    uint   `json:"reads"`
}

type SeqDB struct {
	db             *sql.DB
	stmtAllSeqs    *sql.Stmt
	stmtSearchSeqs *sql.Stmt
	stmtSeqFromId  *sql.Stmt
	dir            string
}

func (tracksDb *SeqDB) Dir() string {
	return tracksDb.dir
}

func NewSeqDB(dir string) *SeqDB {
	db := sys.Must(sql.Open("sqlite3", filepath.Join(dir, "tracks.db?mode=ro")))

	return &SeqDB{dir: dir,
		db:             db,
		stmtAllSeqs:    sys.Must(db.Prepare(ALL_SEQS_SQL)),
		stmtSearchSeqs: sys.Must(db.Prepare(SEARCH_SEQS_SQL)),
		stmtSeqFromId:  sys.Must(db.Prepare(SEQ_FROM_ID_SQL))}
}

func (tracksDb *SeqDB) Genomes() ([]string, error) {
	rows, err := tracksDb.db.Query(GENOMES_SQL)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]string, 0, 10)

	var genome string

	for rows.Next() {
		err := rows.Scan(&genome)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, genome)
	}

	return ret, nil
}
func (tracksDb *SeqDB) Platforms(genome string) ([]string, error) {
	rows, err := tracksDb.db.Query(PLATFORMS_SQL, genome)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]string, 0, 10)

	var platform string

	for rows.Next() {
		err := rows.Scan(&platform)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, platform)
	}

	return ret, nil
}

func (tracksDb *SeqDB) Seqs(genome string, platform string) ([]SeqInfo, error) {
	rows, err := tracksDb.db.Query(TRACKS_SQL, genome, platform)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	defer rows.Close()

	ret := make([]SeqInfo, 0, 10)

	var id uint
	var publicId string
	var name string
	var reads uint
	var stat string
	var dir string

	for rows.Next() {
		err := rows.Scan(&id, &publicId, &genome, &platform, &name, &reads, &stat, &dir)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, SeqInfo{PublicId: publicId, Genome: genome, Platform: platform, Name: name, Reads: reads, Stat: stat})
	}

	return ret, nil
}

func (tracksDb *SeqDB) Search(genome string, query string) ([]SeqInfo, error) {
	var rows *sql.Rows
	var err error

	if query != "" {
		rows, err = tracksDb.stmtSearchSeqs.Query(genome, query, fmt.Sprintf("%%%s%%", query))
	} else {
		rows, err = tracksDb.stmtAllSeqs.Query(genome)
	}

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]SeqInfo, 0, 10)

	var id uint
	var publicId string
	var platform string
	var name string
	var reads uint
	var stat string
	var dir string

	//id, public_id, genome, platform, name, reads, stat_mode, dir

	for rows.Next() {
		err := rows.Scan(&id, &publicId, &genome, &platform, &name, &reads, &stat, &dir)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, SeqInfo{PublicId: publicId, Genome: genome, Platform: platform, Name: name, Reads: reads, Stat: stat})
	}

	return ret, nil
}

func (tracksDb *SeqDB) ReaderFromId(publicId string, binWidth uint, scale float64) (*SeqReader, error) {

	var id uint
	var platform string
	var genome string
	var name string
	var reads uint
	var stat string
	var dir string
	//const FIND_TRACK_SQL = `SELECT platform, genome, name, reads, stat_mode, dir FROM tracks WHERE seq.publicId = ?1`

	err := tracksDb.db.QueryRow(SEQ_FROM_ID_SQL, publicId).Scan(&id, &publicId, &genome, &platform, &name, &reads, &stat, &dir)

	if err != nil {
		return nil, err
	}

	track := Track{Genome: genome, Platform: platform, Name: name}

	dir = filepath.Join(tracksDb.dir, dir)

	return NewSeqReader(dir, track, binWidth, scale)
}

type SeqReader struct {
	Dir     string
	Stat    string
	Track   Track
	BinSize uint
	Reads   uint
	Scale   float64
}

func NewSeqReader(dir string, track Track, binWidth uint, scale float64) (*SeqReader, error) {

	path := filepath.Join(dir, "track.db?mode=ro")

	db, err := sql.Open("sqlite3", path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	var reads uint
	var name string
	var publicId string
	var stat string
	err = db.QueryRow(TRACK_SQL).Scan(&publicId, &name, &reads, &stat)

	if err != nil {
		return nil, err
	}

	// if err != nil {
	// 	return nil, fmt.Errorf("error opening %s", file)
	// }

	// defer file.Close()
	// // Create a scanner
	// scanner := bufio.NewScanner(file)
	// scanner.Scan()

	// count, err := strconv.Atoi(scanner.Text())

	// if err != nil {
	// 	return nil, fmt.Errorf("could not count reads")
	// }

	return &SeqReader{Dir: dir,
		Stat:    stat,
		BinSize: binWidth,
		Reads:   reads,
		Track:   track,
		Scale:   scale}, nil
}

func (reader *SeqReader) getPath(location *dna.Location) string {
	return filepath.Join(reader.Dir, fmt.Sprintf("bin%d", reader.BinSize), fmt.Sprintf("%s_bin%d_%s.db?mode=ro", location.Chr, reader.BinSize, reader.Track.Genome))
}

func (reader *SeqReader) BinCounts(location *dna.Location) (*BinCounts, error) {

	path := reader.getPath(location)

	log.Debug().Msgf("track path %s", path)

	db, err := sql.Open("sqlite3", path)

	if err != nil {
		log.Debug().Msgf("error opening %s %s", path, err)
		return nil, err
	}

	defer db.Close()

	startBin := (location.Start - 1) / reader.BinSize
	endBin := (location.End - 1) / reader.BinSize

	rows, err := db.Query(BIN_SQL,
		startBin,
		endBin)

	if err != nil {
		return nil, err
	}

	var readBlockStart uint
	var readBlockEnd uint
	var count uint
	reads := make([]uint, endBin-startBin+1)
	lastBinOfInterest := startBin + uint(len(reads))

	for rows.Next() {
		err := rows.Scan(&readBlockStart, &readBlockEnd, &count)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		// we don't want to load bin data that goes outside our coordinates
		// of interest. A long gapped bin, may end beyond the blocks we are
		// interested in, so we need to stop the loop short if so.
		endBin := basemath.UintMin(readBlockEnd, lastBinOfInterest)

		// endbin is always 1 past the actual end of the bin, i.e. the start of
		// another bin, therefore we treat it as exclusive
		for bin := readBlockStart; bin < endBin; bin++ {
			reads[bin-startBin] = count // float64(count)
		}
	}

	log.Debug().Msgf("scale reads %f", reader.Scale)

	// scale to some hypothetical .e.g. 1,000,000
	// if reader.Scale > 0 {
	// 	factor := reader.Scale / float64(reader.Reads)

	// 	for i, r := range reads {
	// 		reads[i] = r * factor
	// 	}
	// }

	return &BinCounts{
		Track:    reader.Track,
		Location: location,
		Start:    startBin*reader.BinSize + 1,
		Bins:     reads,
		YMax:     basemath.MaxUintArray(&reads),
		BinWidth: reader.BinSize,
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
