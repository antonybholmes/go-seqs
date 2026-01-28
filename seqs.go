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
	ReadBin struct {
		Start int `json:"s"`
		End   int `json:"e"`
		Count int `json:"c"`
	}

	SampleBinCounts struct {
		Id string `json:"id"`
		//Name    string     `json:"name"`
		Bins    []*ReadBin `json:"bins"`
		YMax    int        `json:"ymax"`
		BinSize int        `json:"binSize"`
		Bpm     float32    `json:"bpmScaleFactor"`
	}

	Platform struct {
		Genome   string `json:"genome"`
		Assembly string `json:"assembly"`
		Platform string `json:"platform"`
	}

	Dataset struct {
		Id string `json:"id"`
		//Genome   string    `json:"genome"`
		Assembly string    `json:"assembly"`
		Platform string    `json:"platform"`
		Name     string    `json:"name"`
		Samples  []*Sample `json:"samples"`
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
	PlatformsSql = `SELECT DISTINCT
		d.id,
		d.genome,
		d.assembly, 
		d.platform
		FROM datasets d
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			p.name IN (<<PERMISSIONS>>)
		ORDER BY
			d.genome,
			d.assembly,
			d.platform`

	DatasetsSql = `SELECT DISTINCT
		d.id,
		d.assembly, 
		d.platform, 	
		d.name
		FROM datasets d
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			p.name IN (<<PERMISSIONS>>)
			AND d.assembly = :assembly
		ORDER BY 
			d.genome,
			d.assembly`

	PlatformDatasetsSql = `SELECT DISTINCT
		d.id,
		d.assembly, 
		d.platform, 	
		d.name
		FROM datasets d
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			p.name IN (<<PERMISSIONS>>)
			AND d.platform = :platform
			AND d.assembly = :assembly
		ORDER BY 
			d.genome,
			d.assembly`

	//const TRACK_SQL = `SELECT name, reads FROM track`

	SelectSampleSql = `SELECT
		s.id,
		s.name,  
		s.reads, 
		s.type, 
		s.url, 
		s.tags`

	CanViewSampleSql = `SELECT
		s.id
		FROM samples s
		JOIN datasets d ON s.dataset_id = d.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE
			s.id = :id AND
			p.name IN (<<PERMISSIONS>>)
			`

	DatasetSamplesSql = SelectSampleSql +
		` FROM samples s
		JOIN datasets d ON s.dataset_id = d.id 
		WHERE d.id = :id
		ORDER BY s.name`

	SampleFromIdSql = SelectSampleSql +
		` FROM samples s 
		JOIN datasets d ON s.dataset_id = d.id
		WHERE id = :id`

	BaseSearchSamplesSql = `SELECT
		d.id as dataset_id,
		d.platform, 	
		d.name as dataset_name,
		s.id as sample_id,
		s.name as sample_name,  
		s.reads, 
		s.type, 
		s.url, 
		s.tags
		FROM samples s
		JOIN datasets d ON s.dataset_id = d.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			p.name IN (<<PERMISSIONS>>)
			AND d.assembly = :assembly`

	AllSamplesSql = BaseSearchSamplesSql +
		` ORDER BY 
			d.platform, 
			d.name, 
			s.name`

	SearchSamplesSql = BaseSearchSamplesSql +
		` AND (s.id = :id OR d.id = :id OR d.platform = :id OR d.name LIKE :q OR s.name LIKE :q)
		ORDER BY 
			d.platform, 
			d.name, 
			s.name`

	SearchPlatformSamplesSql = BaseSearchSamplesSql +
		` AND d.platform = :platform
		AND (s.id = :id OR d.id = :id OR d.name LIKE :q OR s.name LIKE :q)
		ORDER BY
			d.name, 
			s.name`

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

// func (sdb *SeqDB) Genomes(permissions []string) ([]string, error) {
// 	rows, err := sdb.db.Query(DatasetsSql)

// 	if err != nil {
// 		return nil, err //fmt.Errorf("there was an error with the database query")
// 	}

// 	defer rows.Close()

// 	ret := make([]string, 0, 10)

// 	var genome string

// 	for rows.Next() {
// 		err := rows.Scan(&genome)

// 		if err != nil {
// 			return nil, err //fmt.Errorf("there was an error with the database records")
// 		}

// 		ret = append(ret, genome)
// 	}

// 	return ret, nil
// }

func (sdb *SeqDB) CanViewSample(sampleId string, permissions []string) error {
	namedArgs := []any{sql.Named("id", sampleId)}

	inClause := makePermissionsInClause(permissions, &namedArgs)

	query := strings.Replace(CanViewSampleSql, "<<PERMISSIONS>>", inClause, 1)

	var id string
	err := sdb.db.QueryRow(query, namedArgs...).Scan(&id)

	// no rows means no permission
	if err != nil {
		return err
	}

	// sanity
	if id != sampleId {
		return fmt.Errorf("permission denied to view sample %s", sampleId)
	}

	return nil
}

func (sdb *SeqDB) Platforms(assembly string, permissions []string) ([]*Platform, error) {
	namedArgs := make([]any, 0, len(permissions))

	inClause := makePermissionsInClause(permissions, &namedArgs)

	query := strings.Replace(PlatformsSql, "<<PERMISSIONS>>", inClause, 1)

	rows, err := sdb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]*Platform, 0, 10)

	for rows.Next() {
		var platform Platform

		err := rows.Scan(&platform.Genome,
			&platform.Assembly,
			&platform.Platform)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, &platform)
	}

	return ret, nil
}

func (sdb *SeqDB) Datasets(assembly string, permissions []string) ([]*Dataset, error) {
	// build sql.Named args
	namedArgs := []any{sql.Named("assembly", assembly)}

	inClause := makePermissionsInClause(permissions, &namedArgs)

	query := strings.Replace(DatasetsSql, "<<PERMISSIONS>>", inClause, 1)

	// execute query

	rows, err := sdb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]*Dataset, 0, 10)

	for rows.Next() {
		var dataset Dataset

		err := rows.Scan(&dataset.Id,
			&dataset.Assembly,
			&dataset.Platform,
			&dataset.Name)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, &dataset)
	}

	return ret, nil
}

func (sdb *SeqDB) PlatformDatasets(platform string, assembly string, permissions []string) ([]*Dataset, error) {
	// build sql.Named args

	namedArgs := []any{sql.Named("assembly", assembly), sql.Named("platform", platform)}

	inClause := makePermissionsInClause(permissions, &namedArgs)

	query := strings.Replace(DatasetsSql, "<<PERMISSIONS>>", inClause, 1)

	// execute query

	rows, err := sdb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	ret := make([]*Dataset, 0, 10)

	for rows.Next() {
		var dataset Dataset

		err := rows.Scan(&dataset.Id,
			&dataset.Assembly,
			&dataset.Platform,
			&dataset.Name)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		ret = append(ret, &dataset)
	}

	return ret, nil
}

func (sdb *SeqDB) Samples(datasetId string) ([]*Sample, error) {
	rows, err := sdb.db.Query(DatasetSamplesSql, sql.Named("id", datasetId))

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

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

func (sdb *SeqDB) Search(query string, assembly string, permissions []string) ([]*Dataset, error) {

	var rows *sql.Rows
	var err error

	if query != "" {

		namedParams := []any{sql.Named("assembly", assembly),
			sql.Named("id", query),
			sql.Named("q", fmt.Sprintf("%%%s%%", query))}

		inClause := makePermissionsInClause(permissions, &namedParams)

		// if platform != "" {
		// 	// platform specific search
		// 	rows, err = sdb.db.Query(SearchPlatformSamplesSql,
		// 		sql.Named("assembly", assembly),
		// 		sql.Named("platform", platform),
		// 		sql.Named("id", query),
		// 		sql.Named("q", fmt.Sprintf("%%%s%%", query)))

		// } else {
		//search all platforms within assembly
		rows, err = sdb.db.Query(strings.Replace(SearchSamplesSql, "<<PERMISSIONS>>", inClause, 1),
			namedParams...)
		//}
	} else {
		namedParams := []any{sql.Named("assembly", assembly)}

		inClause := makePermissionsInClause(permissions, &namedParams)

		log.Debug().Msgf("search all samples sql %s", strings.Replace(AllSamplesSql, "<<PERMISSIONS>>", inClause, 1))

		rows, err = sdb.db.Query(strings.Replace(AllSamplesSql, "<<PERMISSIONS>>", inClause, 1),
			namedParams...)
	}

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	defer rows.Close()

	datasets := make([]*Dataset, 0, 10)

	//id, uuid, genome, platform, name, reads, stat_mode, url

	var datasetId string
	var platform string
	var name string
	var tags string

	var dataset *Dataset

	for rows.Next() {
		var sample Sample

		err := rows.Scan(
			&datasetId,
			&platform,
			&name,
			&sample.Id,
			&sample.Name,
			&sample.Reads,
			&sample.Type,
			&sample.Url,
			&tags)

		if err != nil {
			return nil, err //fmt.Errorf("there was an error with the database records")
		}

		if dataset == nil || dataset.Id != datasetId {
			dataset = &Dataset{
				Id:       datasetId,
				Assembly: assembly,
				Platform: platform,
				Name:     name,
				Samples:  make([]*Sample, 0, 10),
			}

			datasets = append(datasets, dataset)
		}

		sample.Tags = tagsToList(tags)

		dataset.Samples = append(dataset.Samples, &sample)
	}

	return datasets, nil
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

	return NewSeqReader(sample.Id, url, binWidth, scale)
}

type SeqReader struct {
	sampleId        string
	url             string
	binSize         int
	defaultBinCount int
	//reads           uint
	//scale           float64
}

func NewSeqReader(sampleId string, url string, binSize int, scale float64) (*SeqReader, error) {

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

	return &SeqReader{
		sampleId: sampleId,
		url:      url,
		binSize:  binSize,
		// estimate the number of bins to represent a region
		defaultBinCount: binSize * 4,
	}, nil
}

// func (reader *SeqReader) getPath(location *dna.Location) string {
// 	return filepath.Join(reader.Dir, fmt.Sprintf("bin%d", reader.BinSize), fmt.Sprintf("%s_bin%d_%s.db?mode=ro", location.Chr, reader.BinSize, reader.Track.Genome))
// }

func (reader *SeqReader) SampleBinCounts(location *dna.Location) (*SampleBinCounts, error) {

	//var startBin uint = (location.Start - 1) / reader.BinSize
	//var endBin uint = (location.End - 1) / reader.BinSize

	// we return something for every call, even if data not available
	ret := SampleBinCounts{
		Id: reader.sampleId,
		//Name: reader.sample.Name,
		//Track:    reader.Track,
		//Location: location,
		//Start:    startBin*reader.BinSize + 1,
		//Chr:     location.Chr,
		Bins:    make([]*ReadBin, 0, reader.defaultBinCount),
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
func makePermissionsInClause(permissions []string, namedArgs *[]any) string {
	inPlaceholders := make([]string, len(permissions))

	for i := range permissions {
		inPlaceholders[i] = fmt.Sprintf(":perm%d", i+1)
	}

	for i, perm := range permissions {
		*namedArgs = append(*namedArgs, sql.Named(fmt.Sprintf("perm%d", i+1), perm))
	}

	return strings.Join(inPlaceholders, ",")
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

	sample.Tags = tagsToList(tags)

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

	sample.Tags = tagsToList(tags)

	return &sample, nil
}

func tagsToList(tags string) []string {
	tagList := strings.Split(tags, ",")
	sort.Strings(tagList)
	// trim
	for i, tag := range tagList {
		tagList[i] = strings.TrimSpace(tag)
	}

	return tagList
}
