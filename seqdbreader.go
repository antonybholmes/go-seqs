package seqs

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"sort"

	basemath "github.com/antonybholmes/go-basemath"
	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
)

const (
	TotalBinReadsSql = `SELECT reads FROM bins WHERE size = :bin_size`

	ReadsSql = `SELECT 
		r.start, 
		r.end, 
		r.count 
		FROM reads r
		JOIN bins b ON r.bin_id = b.id
		JOIN chromosomes c ON r.chr_id = c.id
 		WHERE c.name = :chr 
			AND b.size = :bin 
			AND r.start <= :end 
			AND r.end >= :start
		ORDER BY r.start`
)

type DBSeqReader struct {
	sample  *Sample
	url     string
	binSize int
	//defaultBinCount int
	//scale           float64
}

func NewDBSeqReader(sample *Sample, url string, binSize int) (*DBSeqReader, error) {

	return &DBSeqReader{
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

func (reader *DBSeqReader) BinCounts(location *dna.Location) (*SampleBinCounts, error) {

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
		Bins:    make([]*ReadBin, 0, reader.binSize),
		YMax:    0,
		BinSize: reader.binSize,
		//Reads:   reader.sample.Reads,
	}

	// path := filepath.Join(reader.url,
	// 	fmt.Sprintf("%s.db?mode=ro", location.Chr()))

	path := filepath.Join(reader.url + sys.SqliteDSN)

	//log.Debug().Msgf("track path %s", path)

	db, err := sql.Open(sys.Sqlite3DB, path)

	if err != nil {
		return &ret, err
	}

	defer db.Close()

	//var bpmReads int
	//var scaleFactor float64

	err = db.QueryRow(TotalBinReadsSql, reader.binSize).Scan(&ret.BinReads) ///endBin)

	if err != nil {
		log.Debug().Msgf("error scale factor %s %s", path, err)
		return &ret, err
	}

	//ret.BinReads = bpmReads
	//ret.BpmScaleFactor = scaleFactor

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
		sql.Named("chr", location.Chr()),
		sql.Named("bin", reader.binSize),
		sql.Named("start", location.Start()), //	startBin,
		sql.Named("end", location.End()))     ///endBin)

	if err != nil {
		log.Debug().Msgf("error reading reads %s %s", path, err)
		return &ret, err
	}

	for rows.Next() {
		var bin ReadBin
		// read the location
		err := rows.Scan(&bin.Start, &bin.End, &bin.Count)

		if err != nil {
			return &ret, err //fmt.Errorf("there was an error with the database records")
		}

		ret.Bins = append(ret.Bins, &bin)
	}

	for _, bin := range ret.Bins {
		ret.YMax = basemath.Max(ret.YMax, bin.Count)
	}

	return &ret, nil
}

// Creates the IN clause for permissions and appends named args
// for use in sql query so it can be done in a safe way
// func MakePermissionsInClause(permissions []string, namedArgs *[]any) string {
// 	inPlaceholders := make([]string, len(permissions))

// 	for i, perm := range permissions {
// 		ph := fmt.Sprintf("perm%d", i+1)
// 		inPlaceholders[i] = ":" + ph
// 		*namedArgs = append(*namedArgs, sql.Named(ph, perm))
// 	}

// 	return strings.Join(inPlaceholders, ",")
// }

func rowsToSample(rows *sql.Rows) (*Sample, error) {
	var sample Sample

	var tagData []byte

	err := rows.Scan(&sample.Id,
		&sample.Genome,
		&sample.Assembly,
		&sample.Technology,
		&sample.Institution,
		&sample.Dataset,
		&sample.Name,
		&sample.Type,
		&sample.Reads,
		&sample.Url,
		&sample.PublicUrl,
		&tagData)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database records")
	}

	tags, err := TagsToList(tagData)

	if err != nil {
		return nil, err
	}
	sample.Tags = tags

	return &sample, nil
}

func rowToSample(rows *sql.Row) (*Sample, error) {
	var sample Sample
	var tagData []byte

	err := rows.Scan(&sample.Id,
		&sample.Genome,
		&sample.Assembly,
		&sample.Technology,
		&sample.Institution,
		&sample.Dataset,
		&sample.Name,
		&sample.Type,
		&sample.Reads,
		&sample.Url,
		&sample.PublicUrl,
		&tagData)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database records")
	}

	tags, err := TagsToList(tagData)

	if err != nil {
		return nil, err
	}

	sample.Tags = tags

	return &sample, nil
}

func TagsToList(data []byte) ([]Tag, error) {
	if len(data) == 0 {
		return []Tag{}, nil
	}

	var tags []Tag
	err := json.Unmarshal(data, &tags)
	if err != nil {
		log.Debug().Msgf("error unmarshalling tags: %s", err)
		return []Tag{}, err
	}

	// sort tags by name
	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Name < tags[j].Name
	})

	return tags, nil
}
