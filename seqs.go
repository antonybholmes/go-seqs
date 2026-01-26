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
	"github.com/antonybholmes/go-sys/log"
	_ "github.com/mattn/go-sqlite3"
)

type (
	SeqBin struct {
		Start int `json:"s"`
		End   int `json:"e"`
		Reads int `json:"r"`
	}

	SampleBinCounts struct {
		Id   string `json:"id"`
		Name string `json:"name"`
		//Chr string `json:"chr"`
		//Track    Track         `json:"track"`
		//Location *dna.Location `json:"loc"`
		//Bins []*SeqBin `json:"bins"`
		Bins [][]int `json:"bins"`
		YMax int     `json:"ymax"`
		//Start    uint          `json:"start"`
		BinSize int     `json:"binSize"`
		Bpm     float32 `json:"bpmScaleFactor"`
	}

	// type Sample struct {
	// 	Genome   string `json:"genome"`
	// 	Platform string `json:"platform"`
	// 	Dataset  string `json:"dataset"`
	// 	Name     string `json:"name"`
	// }

	Dataset struct {
		Id       string `json:"id"`
		Genome   string `json:"genome"`
		Assembly string `json:"assembly"`
		Platform string `json:"platform"`
		Name     string `json:"name"`
	}

	Sample struct {
		Id string `json:"id"`
		//Genome    string   `json:"genome"`
		//Assembly  string   `json:"assembly"`
		//Platform  string   `json:"platform"`
		//Dataset   string   `json:"dataset"`
		Name  string   `json:"name"`
		Type  string   `json:"type"`
		Url   string   `json:"url"`
		Tags  []string `json:"tags"`
		Reads uint     `json:"reads"`
	}

	SeqDB struct {
		db  *sql.DB
		url string
	}
)

// const MAGIC_NUMBER_OFFSET_BYTES = 0
// const BIN_SIZE_OFFSET_BYTES = MAGIC_NUMBER_OFFSET_BYTES + 4
// const BIN_WIDTH_OFFSET_BYTES = BIN_SIZE_OFFSET_BYTES + 4
// const N_BINS_OFFSET_BYTES = BIN_WIDTH_OFFSET_BYTES + 4
// const BINS_OFFSET_BYTES = N_BINS_OFFSET_BYTES + 4

const (
	GenomesSql   = `SELECT DISTINCT genome FROM datasets ORDER BY genome`
	PlatformsSql = `SELECT DISTINCT platform FROM datasets WHERE genome = :genome ORDER BY platform`

	//const TRACK_SQL = `SELECT name, reads FROM track`

	SelectSampleSql = `SELECT 
		d.id as dataset_id,
		d.genome, 
		d.assembly, 
		d.platform, 	
		d.name as dataset_name,
		s.id,
		s.name, 
		s.reads, 
		s.type, 
		s.url, 
		s.tags`

	SamplesSql = SelectSampleSql +
		` FROM samples s
		JOIN datasets d ON s.dataset_id = d.id 
		WHERE d.genome = :genome AND d.platform = :platform 
		ORDER BY s.name`

	AllSamplesSql = SelectSampleSql +
		` FROM samples s 
		JOIN datasets d ON s.dataset_id = d.id
		WHERE d.genome = :genome
		ORDER BY 
			d.genome, 
			d.platform, 
			d.name, 
			s.name`

	SampleFromIdSql = SelectSampleSql +
		` FROM samples s 
		JOIN datasets d ON s.dataset_id = d.id
		WHERE id = :id`

	SearchSamplesSql = SelectSampleSql +
		` FROM samples s
		JOIN datasets d ON s.dataset_id = d.id
		WHERE 
			genome = :genome AND 
			(id = :id OR d.platform = :id OR d.dataset LIKE :q OR s.name LIKE :q)
		ORDER BY d.genome, d.platform, d.name, s.name`

	ReadsSql = `SELECT start, end, count 
		FROM bins
 		WHERE bin=:bin AND start <= :end AND end >= :start
		ORDER BY start`

	BpmSql = `SELECT bpm_scale_factor FROM bins WHERE size = :bin_size`
)

func (sdb *SeqDB) Dir() string {
	return sdb.url
}

func NewSeqDB(url string) *SeqDB {
	log.Debug().Msgf("Load db: %s", filepath.Join(url, "samples.db?mode=ro"))
	db := sys.Must(sql.Open(sys.Sqlite3DB, filepath.Join(url, "samples.db?mode=ro")))

	//x := sys.Must(db.Prepare(ALL_TRACKS_SQL))

	return &SeqDB{url: url, db: db}
}

func (sdb *SeqDB) Close() error {
	return sdb.db.Close()
}

func (sdb *SeqDB) Genomes() ([]string, error) {
	rows, err := sdb.db.Query(GenomesSql)

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
func (sdb *SeqDB) Platforms(genome string) ([]string, error) {
	rows, err := sdb.db.Query(PlatformsSql, genome)

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

func (sdb *SeqDB) Seqs(genome string, platform string) ([]*Sample, error) {
	rows, err := sdb.db.Query(SamplesSql, genome, platform)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	defer rows.Close()

	ret := make([]*Sample, 0, 10)

	for rows.Next() {
		sample, err := rowsToSample(rows)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, sample)
	}

	return ret, nil
}

func (sdb *SeqDB) Search(genome string, query string) ([]*Sample, error) {
	var rows *sql.Rows
	var err error

	if query != "" {
		rows, err = sdb.db.Query(SearchSamplesSql,
			sql.Named("genome", genome),
			sql.Named("id", query),
			sql.Named("q", fmt.Sprintf("%%%s%%", query)))
	} else {
		rows, err = sdb.db.Query(AllSamplesSql, genome)
	}

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]*Sample, 0, 10)

	//id, uuid, genome, platform, name, reads, stat_mode, url

	for rows.Next() {
		sample, err := rowsToSample(rows)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		// if sample.Type == "Remote BigWig" {
		// 	sample.Url = url
		// }

		ret = append(ret, sample)
	}

	return ret, nil
}

func (sdb *SeqDB) ReaderFromId(sampleId string, binWidth int, scale float64) (*SeqReader, error) {

	var url string

	//const FIND_TRACK_SQL = `SELECT platform, genome, name, reads, stat_mode, url FROM tracks WHERE seq.publicId = ?1`

	row := sdb.db.QueryRow(SampleFromIdSql, sql.Named("id", sampleId))

	sample, err := rowToSample(row)

	if err != nil {
		return nil, err
	}

	url = filepath.Join(sdb.url, url)

	return NewSeqReader(url, sample, binWidth, scale)
}

type SeqReader struct {
	url             string
	sample          *Sample
	binSize         int
	defaultBinCount int
	//reads           uint
	//scale           float64
}

func NewSeqReader(url string, sample *Sample, binSize int, scale float64) (*SeqReader, error) {

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
		sample:          sample,
	}, nil
}

// func (reader *SeqReader) getPath(location *dna.Location) string {
// 	return filepath.Join(reader.Dir, fmt.Sprintf("bin%d", reader.BinSize), fmt.Sprintf("%s_bin%d_%s.db?mode=ro", location.Chr, reader.BinSize, reader.Track.Genome))
// }

func (reader *SeqReader) TrackBinCounts(location *dna.Location) (*SampleBinCounts, error) {

	//var startBin uint = (location.Start - 1) / reader.BinSize
	//var endBin uint = (location.End - 1) / reader.BinSize

	// we return something for every call, even if data not available
	ret := SampleBinCounts{
		Id:   reader.sample.Id,
		Name: reader.sample.Name,
		//Track:    reader.Track,
		//Location: location,
		//Start:    startBin*reader.BinSize + 1,
		//Chr:     location.Chr,
		Bins:    make([][]int, 0, reader.defaultBinCount),
		YMax:    0,
		BinSize: reader.binSize,
		Bpm:     0,
	}

	path := filepath.Join(reader.url,
		fmt.Sprintf("%s.db?mode=ro", location.Chr()))

	//log.Debug().Msgf("track path %s", path)

	db, err := sql.Open(sys.Sqlite3DB, path)

	if err != nil {
		log.Debug().Msgf("error opening %s %s", path, err)
		return &ret, err
	}

	defer db.Close()

	var bpm float32

	err = db.QueryRow(BpmSql, reader.binSize).Scan(&bpm) ///endBin)

	if err != nil {
		return &ret, err
	}

	ret.Bpm = bpm

	//var binSql string

	// switch reader.binSize {
	// case 16:
	// 	binSql = BIN_16_SQL
	// case 64:
	// 	binSql = BIN_64_SQL
	// case 256:
	// 	binSql = BIN_256_SQL
	// case 1024:
	// 	binSql = BIN_1024_SQL
	// case 4096:
	// 	binSql = BIN_4096_SQL
	// default:
	// 	binSql = BIN_16384_SQL
	// }

	rows, err := db.Query(ReadsSql,
		sql.Named("start", location.Start()), //	startBin,
		sql.Named("end", location.End()))     ///endBin)

	if err != nil {
		return &ret, err
	}

	var readStart int
	var readEnd int
	var reads int

	for rows.Next() {
		// read the location
		err := rows.Scan(&readStart, &readEnd, &reads)

		if err != nil {
			return &ret, err //fmt.Errorf("there was an error with the database records")
		}

		// to reduce data overhead, return 3 element array of start, end and read count
		ret.Bins = append(ret.Bins, []int{readStart, readEnd, reads})
	}

	for _, bin := range ret.Bins {
		ret.YMax = basemath.Max(ret.YMax, bin[2])
	}

	return &ret, nil
}

func rowsToSample(rows *sql.Rows) (*Sample, error) {
	var sample Sample
	var tags string

	err := rows.Scan(&sample.Id,
		&sample.Name,
		&sample.Reads,
		&sample.Type,
		&sample.Url,
		&tags)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database records")
	}

	tagList := strings.Split(tags, ",")
	sort.Strings(tagList)

	sample.Tags = tagList

	return &sample, nil
}

func rowToSample(rows *sql.Row) (*Sample, error) {
	var sample Sample
	var tags string

	err := rows.Scan(&sample.Id,
		&sample.Name,
		&sample.Reads,
		&sample.Type,
		&sample.Url,
		&tags)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database records")
	}

	tagList := strings.Split(tags, ",")
	sort.Strings(tagList)

	sample.Tags = tagList

	return &sample, nil
}
