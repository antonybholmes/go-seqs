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

const SELECT_TRACK_SQL = `SELECT id, public_id, genome, platform, dataset, name, reads, track_type, url, tags `

const TRACKS_SQL = SELECT_TRACK_SQL +
	`FROM tracks 
	WHERE genome = ?1 AND platform = ?2 
	ORDER BY name`

const ALL_TRACKS_SQL = SELECT_TRACK_SQL +
	`FROM tracks 
	WHERE genome = ?1 
	ORDER BY genome, platform, dataset, name`

const TRACK_FROM_ID_SQL = SELECT_TRACK_SQL +
	`FROM tracks 
	WHERE public_id = ?1`

const SEARCH_TRACKS_SQL = SELECT_TRACK_SQL +
	`FROM tracks 
	WHERE genome = ?1 AND (public_id = ?1 OR platform = ?1 OR dataset LIKE ?2 OR name LIKE ?2)
	ORDER BY genome, platform, dataset, name`

const BIN_SQL = `SELECT start, end, reads 
	FROM bins
 	WHERE start <= ?2 AND end >= ?1
	ORDER BY start`

const BIN_50_SQL = `SELECT start, end, reads 
	FROM bins50
 	WHERE start <= ?2 AND end >= ?1
	ORDER BY start`

const BIN_500_SQL = `SELECT start, end, reads 
	FROM bins500
 	WHERE start <= ?2 AND end >= ?1
	ORDER BY start`

const BIN_5000_SQL = `SELECT start, end, reads 
	FROM bins5000
 	WHERE start <= ?2 AND end >= ?1
	ORDER BY start`

// const BIN_20_SQL = `SELECT start, end, reads
// 	FROM bins20
//  	WHERE start <= ?2 AND end >= ?1
// 	ORDER BY start`

// const BIN_200_SQL = `SELECT start, end, reads
// 	FROM bins200
//  	WHERE start <= ?2 AND end >= ?1
// 	ORDER BY start`

// const BIN_2000_SQL = `SELECT start, end, reads
// 	FROM bins2000
//  	WHERE start <= ?2 AND end >= ?1
// 	ORDER BY start`

// const BIN_20000_SQL = `SELECT start, end, reads
// 	FROM bins20000
//  	WHERE start <= ?2 AND end >= ?1
// 	ORDER BY start`

// const BIN_10_SQL = `SELECT start, end, reads
// 	FROM bins10
//  	WHERE start <= ?2 AND end >= ?1
// 	ORDER BY start`

// const BIN_100_SQL = `SELECT start, end, reads
// 	FROM bins100
//  	WHERE start <= ?2 AND end >= ?1
// 	ORDER BY start`

// const BIN_1000_SQL = `SELECT start, end, reads
// 	FROM bins1000
//  	WHERE start <= ?2 AND end >= ?1
// 	ORDER BY start`

// const BIN_10000_SQL = `SELECT start, end, reads
// 	FROM bins10000
//  	WHERE start <= ?2 AND end >= ?1
// 	ORDER BY start`

const BIN_16_SQL = `SELECT start, end, reads 
	FROM bins16
 	WHERE start <= ?2 AND end >= ?1
	ORDER BY start`

const BIN_64_SQL = `SELECT start, end, reads 
	FROM bins64
 	WHERE start <= ?2 AND end >= ?1
	ORDER BY start`

const BIN_256_SQL = `SELECT start, end, reads 
	FROM bins256
 	WHERE start <= ?2 AND end >= ?1
	ORDER BY start`

const BIN_1024_SQL = `SELECT start, end, reads 
	FROM bins1024
 	WHERE start <= ?2 AND end >= ?1
	ORDER BY start`

const BIN_4096_SQL = `SELECT start, end, reads 
	FROM bins4096
 	WHERE start <= ?2 AND end >= ?1
	ORDER BY start`

const BIN_16384_SQL = `SELECT start, end, reads 
	FROM bins16384
 	WHERE start <= ?2 AND end >= ?1
	ORDER BY start`

const BPM_SQL = `SELECT scale_factor FROM bpm_scale_factors WHERE bin_size = ?1`

type SeqBin struct {
	Start uint `json:"s"`
	End   uint `json:"e"`
	Reads uint `json:"r"`
}

type TrackBinCounts struct {
	PublicId string `json:"publicId"`
	Name     string `json:"name"`
	//Chr string `json:"chr"`
	//Track    Track         `json:"track"`
	//Location *dna.Location `json:"loc"`
	//Bins []*SeqBin `json:"bins"`
	Bins [][]uint `json:"bins"`
	YMax uint     `json:"ymax"`
	//Start    uint          `json:"start"`
	BinSize uint    `json:"binSize"`
	Bpm     float32 `json:"bpmScaleFactor"`
}

// type Track struct {
// 	Genome   string `json:"genome"`
// 	Platform string `json:"platform"`
// 	Dataset  string `json:"dataset"`
// 	Name     string `json:"name"`
// }

type Track struct {
	PublicId  string   `json:"publicId"`
	Genome    string   `json:"genome"`
	Platform  string   `json:"platform"`
	Dataset   string   `json:"dataset"`
	Name      string   `json:"name"`
	TrackType string   `json:"trackType"`
	Url       string   `json:"url"`
	Tags      []string `json:"tags"`
	Reads     uint     `json:"reads"`
}

type SeqDB struct {
	db *sql.DB
	//stmtAllSeqs    *sql.Stmt
	//stmtSearchSeqs *sql.Stmt
	//stmtSeqFromId  *sql.Stmt
	url string
}

func (tracksDb *SeqDB) Dir() string {
	return tracksDb.url
}

func NewSeqDB(url string) *SeqDB {
	log.Debug().Msgf("Load db: %s", filepath.Join(url, "tracks.db?mode=ro"))
	db := sys.Must(sql.Open("sqlite3", filepath.Join(url, "tracks.db?mode=ro")))

	//x := sys.Must(db.Prepare(ALL_TRACKS_SQL))

	return &SeqDB{url: url,
		db: db,
		//stmtAllSeqs:    x,
		//stmtSearchSeqs: sys.Must(db.Prepare(SEARCH_TRACKS_SQL)),
		//stmtSeqFromId:  sys.Must(db.Prepare(TRACK_FROM_ID_SQL))
	}
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
	var trackType string
	var reads uint
	var url string
	var tags string

	for rows.Next() {
		err := rows.Scan(&id,
			&publicId,
			&genome,
			&platform,
			&dataset,
			&name,
			&reads,
			&trackType,
			&url,
			&tags)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		tagList := strings.Split(tags, ",")
		sort.Strings(tagList)

		track := Track{PublicId: publicId,
			Genome:    genome,
			Platform:  platform,
			Dataset:   dataset,
			Name:      name,
			Reads:     reads,
			TrackType: trackType,
			Tags:      tagList}

		if track.TrackType == "Remote BigWig" {
			track.Url = url
		}

		ret = append(ret, track)
	}

	return ret, nil
}

func (tracksDb *SeqDB) Search(genome string, query string) ([]Track, error) {
	var rows *sql.Rows
	var err error

	if query != "" {
		rows, err = tracksDb.db.Query(SEARCH_TRACKS_SQL, genome, query, fmt.Sprintf("%%%s%%", query))
	} else {
		rows, err = tracksDb.db.Query(ALL_TRACKS_SQL, genome)
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
	var trackType string
	var url string
	var tags string

	//id, uuid, genome, platform, name, reads, stat_mode, url

	for rows.Next() {
		err := rows.Scan(&id, &publicId, &genome, &platform, &dataset, &name, &reads, &trackType, &url, &tags)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		tagList := strings.Split(tags, ",")
		sort.Strings(tagList)

		track := Track{PublicId: publicId,
			Genome:    genome,
			Platform:  platform,
			Dataset:   dataset,
			Name:      name,
			Reads:     reads,
			TrackType: trackType,
			Tags:      tagList}

		if track.TrackType == "Remote BigWig" {
			track.Url = url
		}

		ret = append(ret, track)
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
	var trackType string
	var url string
	var tags string

	//const FIND_TRACK_SQL = `SELECT platform, genome, name, reads, stat_mode, url FROM tracks WHERE seq.publicId = ?1`

	err := tracksDb.db.QueryRow(TRACK_FROM_ID_SQL, publicId).Scan(&id,
		&publicId,
		&genome,
		&platform,
		&dataset,
		&name,
		&reads,
		&trackType,
		&url,
		&tags)

	if err != nil {
		return nil, err
	}

	tagList := strings.Split(tags, ",")
	sort.Strings(tagList)

	track := Track{PublicId: publicId, Genome: genome, Platform: platform, Dataset: dataset, Name: name, TrackType: trackType, Tags: tagList}

	if track.TrackType == "Remote bigWig" {
		track.Url = url
	}

	url = filepath.Join(tracksDb.url, url)

	return NewSeqReader(url, track, binWidth, scale)
}

type SeqReader struct {
	url             string
	track           Track
	binSize         uint
	defaultBinCount uint
	//reads           uint
	//scale           float64
}

func NewSeqReader(url string, track Track, binSize uint, scale float64) (*SeqReader, error) {

	// path := filepath.Join(url, "track.db?mode=ro")

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

	return &SeqReader{url: url,
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

func (reader *SeqReader) TrackBinCounts(location *dna.Location) (*TrackBinCounts, error) {

	//var startBin uint = (location.Start - 1) / reader.BinSize
	//var endBin uint = (location.End - 1) / reader.BinSize

	// we return something for every call, even if data not available
	ret := TrackBinCounts{
		PublicId: reader.track.PublicId,
		Name:     reader.track.Name,
		//Track:    reader.Track,
		//Location: location,
		//Start:    startBin*reader.BinSize + 1,
		//Chr:     location.Chr,
		Bins:    make([][]uint, 0, reader.defaultBinCount),
		YMax:    0,
		BinSize: reader.binSize,
		Bpm:     0,
	}

	path := filepath.Join(reader.url,
		fmt.Sprintf("%s_%s.db?mode=ro", location.Chr, reader.track.Genome))

	//log.Debug().Msgf("track path %s", path)

	db, err := sql.Open("sqlite3", path)

	if err != nil {
		log.Debug().Msgf("error opening %s %s", path, err)
		return &ret, err
	}

	defer db.Close()

	var bpm float32

	err = db.QueryRow(BPM_SQL, reader.binSize).Scan(&bpm) ///endBin)

	if err != nil {
		return &ret, err
	}

	ret.Bpm = bpm

	var binSql string

	switch reader.binSize {
	case 16:
		binSql = BIN_16_SQL
	case 64:
		binSql = BIN_64_SQL
	case 256:
		binSql = BIN_256_SQL
	case 1024:
		binSql = BIN_1024_SQL
	case 4096:
		binSql = BIN_4096_SQL
	default:
		binSql = BIN_16384_SQL
	}

	rows, err := db.Query(binSql,
		location.Start, //	startBin,
		location.End)   ///endBin)

	if err != nil {
		return &ret, err
	}

	var readStart uint
	var readEnd uint
	var reads uint

	for rows.Next() {
		// read the location
		err := rows.Scan(&readStart, &readEnd, &reads)

		if err != nil {
			return &ret, err //fmt.Errorf("there was an error with the database records")
		}

		// to reduce data overhead, return 3 element array of start, end and read count
		ret.Bins = append(ret.Bins, []uint{readStart, readEnd, reads})
	}

	for _, bin := range ret.Bins {
		ret.YMax = basemath.Max(ret.YMax, bin[2])
	}

	return &ret, nil
}
