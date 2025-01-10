package seqs

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

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

//const TRACK_SQL = `SELECT name, reads FROM track`

const SELECT_TRACK_SQL = `SELECT id, public_id, genome, platform, dataset, name, reads, dir, tags `

const TRACKS_SQL = SELECT_TRACK_SQL +
	`FROM tracks 
	WHERE genome = ?1 AND platform = ?2 
	ORDER BY name`

const ALL_SEQS_SQL = SELECT_TRACK_SQL +
	`FROM tracks 
	WHERE genome = ?1 
	ORDER BY genome, platform, dataset, name`

const SEQ_FROM_ID_SQL = SELECT_TRACK_SQL +
	`FROM tracks 
	WHERE public_id = ?1`

const SEARCH_SEQS_SQL = SELECT_TRACK_SQL +
	`FROM tracks 
	WHERE genome = ?1 AND (public_id = ?1 OR platform = ?1 OR dataset LIKE ?2 OR name LIKE ?2)
	ORDER BY genome, platform, dataset, name`

const BIN_SQL = `SELECT start, end, reads 
	FROM bins
 	WHERE start <= ?2 AND end >= ?1
	ORDER BY start`

const BPM_SQL = `SELECT bpm_scale_factor FROM track`

type SeqBin struct {
	Start uint `json:"s"`
	End   uint `json:"e"`
	Reads uint `json:"r"`
}

type BinCounts struct {
	Chr string `json:"chr"`
	//Track    Track         `json:"track"`
	//Location *dna.Location `json:"loc"`
	//Bins []*SeqBin `json:"bins"`
	Bins [][]uint `json:"bins"`
	YMax uint     `json:"ymax"`
	//Start    uint          `json:"start"`
	BinWidth uint    `json:"binWidth"`
	Bpm      float32 `json:"bpmScaleFactor"`
}

// type Track struct {
// 	Genome   string `json:"genome"`
// 	Platform string `json:"platform"`
// 	Dataset  string `json:"dataset"`
// 	Name     string `json:"name"`
// }

type Track struct {
	PublicId string   `json:"seqId"`
	Genome   string   `json:"genome"`
	Platform string   `json:"platform"`
	Dataset  string   `json:"dataset"`
	Name     string   `json:"name"`
	Tags     []string `json:"tags"`
	Reads    uint     `json:"reads"`
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
	log.Debug().Msgf("Load db: %s", filepath.Join(dir, "tracks.db?mode=ro"))
	db := sys.Must(sql.Open("sqlite3", filepath.Join(dir, "tracks.db?mode=ro")))

	x := sys.Must(db.Prepare(ALL_SEQS_SQL))

	return &SeqDB{dir: dir,
		db:             db,
		stmtAllSeqs:    x,
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

func (tracksDb *SeqDB) Seqs(genome string, platform string) ([]Track, error) {
	rows, err := tracksDb.db.Query(TRACKS_SQL, genome, platform)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	defer rows.Close()

	ret := make([]Track, 0, 10)

	var id uint
	var publicId string
	var dataset string
	var name string
	var reads uint
	var dir string
	var tags string

	for rows.Next() {
		err := rows.Scan(&id, &publicId, &genome, &platform, &dataset, &name, &reads, &dir, &tags)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		tagList := strings.Split(tags, ",")
		sort.Strings(tagList)

		ret = append(ret, Track{PublicId: publicId,
			Genome:   genome,
			Platform: platform,
			Dataset:  dataset,
			Name:     name,
			Reads:    reads,
			Tags:     tagList})
	}

	return ret, nil
}

func (tracksDb *SeqDB) Search(genome string, query string) ([]Track, error) {
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

	ret := make([]Track, 0, 10)

	var id uint
	var publicId string
	var platform string
	var dataset string
	var name string
	var reads uint
	var dir string
	var tags string

	//id, uuid, genome, platform, name, reads, stat_mode, dir

	for rows.Next() {
		err := rows.Scan(&id, &publicId, &genome, &platform, &dataset, &name, &reads, &dir, &tags)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		tagList := strings.Split(tags, ",")
		sort.Strings(tagList)

		ret = append(ret, Track{PublicId: publicId,
			Genome:   genome,
			Platform: platform,
			Dataset:  dataset,
			Name:     name,
			Reads:    reads,
			Tags:     tagList})
	}

	return ret, nil
}

func (tracksDb *SeqDB) ReaderFromId(publicId string, binWidth uint, scale float64) (*SeqReader, error) {

	var id uint
	var platform string
	var genome string
	var dataset string
	var name string
	var reads uint
	var dir string
	var tags string

	//const FIND_TRACK_SQL = `SELECT platform, genome, name, reads, stat_mode, dir FROM tracks WHERE seq.publicId = ?1`

	err := tracksDb.db.QueryRow(SEQ_FROM_ID_SQL, publicId).Scan(&id,
		&publicId,
		&genome,
		&platform,
		&dataset,
		&name,
		&reads,
		&dir,
		&tags)

	if err != nil {
		return nil, err
	}

	tagList := strings.Split(tags, ",")
	sort.Strings(tagList)

	track := Track{Genome: genome, Platform: platform, Dataset: dataset, Name: name, Tags: tagList}

	dir = filepath.Join(tracksDb.dir, dir)

	return NewSeqReader(dir, track, binWidth, scale)
}

type SeqReader struct {
	dir             string
	track           Track
	binSize         uint
	defaultBinCount uint
	//reads           uint
	//scale           float64
}

func NewSeqReader(dir string, track Track, binSize uint, scale float64) (*SeqReader, error) {

	// path := filepath.Join(dir, "track.db?mode=ro")

	// db, err := sql.Open("sqlite3", path)

	// if err != nil {
	// 	return nil, err
	// }

	// defer db.Close()

	// var reads uint
	// var name string
	// err = db.QueryRow(TRACK_SQL).Scan(&name, &reads)

	// if err != nil {
	// 	return nil, err
	// }

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

	return &SeqReader{dir: dir,
		binSize: binSize,
		// estimate the number of bins to represent a region
		defaultBinCount: binSize * 4,
		//reads:           reads,
		track: track,
		//scale:           scale
	}, nil
}

// func (reader *SeqReader) getPath(location *dna.Location) string {
// 	return filepath.Join(reader.Dir, fmt.Sprintf("bin%d", reader.BinSize), fmt.Sprintf("%s_bin%d_%s.db?mode=ro", location.Chr, reader.BinSize, reader.Track.Genome))
// }

func (reader *SeqReader) BinCounts(location *dna.Location) (*BinCounts, error) {

	//var startBin uint = (location.Start - 1) / reader.BinSize
	//var endBin uint = (location.End - 1) / reader.BinSize

	// we return something for every call, even if data not available
	ret := BinCounts{
		//Track:    reader.Track,
		//Location: location,
		//Start:    startBin*reader.BinSize + 1,
		Chr:      location.Chr,
		Bins:     make([][]uint, 0, reader.defaultBinCount),
		YMax:     0,
		BinWidth: reader.binSize,
		Bpm:      0,
	}

	path := filepath.Join(reader.dir,
		fmt.Sprintf("bin%d", reader.binSize),
		fmt.Sprintf("%s_bin%d_%s.db?mode=ro", location.Chr, reader.binSize, reader.track.Genome))

	//log.Debug().Msgf("track path %s", path)

	db, err := sql.Open("sqlite3", path)

	if err != nil {
		log.Debug().Msgf("error opening %s %s", path, err)
		return &ret, err
	}

	defer db.Close()

	var bpm float32

	err = db.QueryRow(BPM_SQL).Scan(&bpm) ///endBin)

	if err != nil {
		log.Debug().Msgf("aha %s %s", path, err)
		return &ret, err
	}

	ret.Bpm = bpm

	rows, err := db.Query(BIN_SQL,
		location.Start, //	startBin,
		location.End)   ///endBin)

	if err != nil {
		return &ret, err
	}

	var readStart uint
	var readEnd uint
	//var readBlockStart uint
	//var readBlockEnd uint
	var reads uint
	//reads := make([]uint, endBin-startBin+1)
	//lastBinOfInterest := startBin + uint(len(ret.Bins))

	for rows.Next() {
		// read the location
		err := rows.Scan(&readStart, &readEnd, &reads)

		if err != nil {
			return &ret, err //fmt.Errorf("there was an error with the database records")
		}

		// to reduce data overhead, return 3 element array of start, end and read count
		ret.Bins = append(ret.Bins, []uint{readStart, readEnd, reads}) //&SeqBin{Start: readStart, End: readEnd, Reads: reads})

		// translate to block coordinates
		//readBlockStart = (readStart - 1) / reader.BinSize
		//readBlockEnd = (readEnd - 1) / reader.BinSize

		// if the bin starts before our region of interest, but the end overlaps into it
		// then we must fix the start to be at least the start bin
		//readBlockStart := basemath.Max(startBin, readBlockStart)

		// we don't want to load bin data that goes outside our coordinates
		// of interest. A long gapped bin, may end beyond the blocks we are
		// interested in, so we need to stop the loop short if so.
		//readBlockEnd := basemath.Min(readBlockEnd, lastBinOfInterest)

		// endbin is always 1 past the actual end of the bin, i.e. the start of
		// another bin, therefore we treat it as exclusive
		//for bin := readBlockStart; bin < readBlockEnd; bin++ {
		//	ret.Bins[bin-startBin] = count // float64(count)
		//}
	}

	//log.Debug().Msgf("scale reads %f", reader.Scale)

	for _, bin := range ret.Bins {
		ret.YMax = basemath.Max(ret.YMax, bin[2])
	}

	// work out ymax
	//ret.YMax = basemath.MaxUintArray(&ret.Bins)

	// scale to some hypothetical .e.g. 1,000,000
	// if reader.Scale > 0 {
	// 	factor := reader.Scale / float64(reader.Reads)

	// 	for i, r := range reads {
	// 		reads[i] = r * factor
	// 	}
	// }

	//log.Debug().Msgf("bins %v", ret)

	return &ret, nil

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
