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
	"github.com/antonybholmes/go-web"
	"github.com/antonybholmes/go-web/auth/sqlite"
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
		//BpmScaleFactor float64    `json:"bpmScaleFactor"`
		Reads    int `json:"reads"`
		BinReads int `json:"binReads"`
	}

	Platform struct {
		Genome   string `json:"genome"`
		Assembly string `json:"assembly"`
		Platform string `json:"platform"`
	}

	Dataset struct {
		Id       string    `json:"id"`
		Genome   string    `json:"genome"`
		Assembly string    `json:"assembly"`
		Platform string    `json:"platform"`
		Name     string    `json:"name"`
		Samples  []*Sample `json:"samples"`
	}

	Sample struct {
		Id         string   `json:"id"`
		Genome     string   `json:"genome"`
		Assembly   string   `json:"assembly"`
		Technology string   `json:"technology"`
		Dataset    string   `json:"dataset"`
		Name       string   `json:"name"`
		Type       string   `json:"type"`
		Url        string   `json:"url"`
		Tags       []string `json:"tags"`
		Reads      int      `json:"reads"`
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
	TechnologiesSql = `SELECT DISTINCT
		d.public_id,
		g.name as genome,
		a.name as assembly, 
		t.name as technology
		FROM datasets d
		JOIN assemblies a ON d.assembly_id = a.id
		JOIN genomes g ON a.genome_id = g.id
		JOIN technologies t ON d.technology_id = t.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			<<PERMISSIONS>>
			AND LOWER(a.name) = :assembly
		ORDER BY
			g.name,
			a.name,
			t.name`

	DatasetsSql = `SELECT DISTINCT
		d.public_id,
		g.name as genome,
		a.name as assembly, 
		t.name as technology,
		d.name
		FROM datasets d
		JOIN assemblies a ON d.assembly_id = a.id
		JOIN genomes g ON a.genome_id = g.id
		JOIN technologies t ON d.technology_id = t.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			<<PERMISSIONS>>
			AND LOWER(a.name) = :assembly
		ORDER BY
			g.name, 
			a.name`

	TechnologyDatasetsSql = `SELECT DISTINCT
		d.public_id,
		a.name as assembly, 
		t.name as technology, 	
		d.name
		FROM datasets d
		JOIN assemblies a ON d.assembly_id = a.id
		JOIN technologies t ON d.technology_id = t.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			<<PERMISSIONS>>
			AND t.name = :technology
			AND LOWER(a.name) = :assembly
		ORDER BY
			a.name,
			d.name`

	//const TRACK_SQL = `SELECT name, reads FROM track`

	CanViewSampleSql = `SELECT DISTINCT
		s.public_id
		FROM samples s
		JOIN datasets d ON s.dataset_id = d.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE
			<<PERMISSIONS>>
			AND s.public_id = :id`

	SelectSampleSql = `SELECT DISTINCT
		s.public_id,
		g.name AS genome,
		a.name AS assembly,
		t.name AS technology, 	
		d.name AS dataset_name,
		s.name AS sample_name,
		st.name AS sample_type, 
		s.reads,
		s.url, 
		s.tags
		FROM samples s
		JOIN datasets d ON s.dataset_id = d.id
		JOIN assemblies a ON d.assembly_id = a.id
		JOIN genomes g ON a.genome_id = g.id
		JOIN technologies t ON d.technology_id = t.id
		JOIN sample_types st ON s.type_id = st.id`

	DatasetSamplesSql = SelectSampleSql +
		` WHERE d.public_id = :id
		ORDER BY s.name`

	SampleFromIdSql = SelectSampleSql +
		` WHERE s.public_id = :id`

	BaseSearchSamplesSql = SelectSampleSql +
		` JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			<<PERMISSIONS>>
			AND LOWER(a.name) = :assembly`

	AllSamplesSql = BaseSearchSamplesSql +
		` ORDER BY 
			t.name, 
			d.name, 
			s.name`

	SearchSamplesSql = BaseSearchSamplesSql +
		` AND (s.public_id = :id OR d.public_id = :id OR a.name = :id OR d.name LIKE :q OR s.name LIKE :q)
		ORDER BY 
			t.name, 
			d.name, 
			s.name`

	// SearchPlatformSamplesSql = BaseSearchSamplesSql +
	// 	` AND d.platform = :platform
	// 	AND (s.id = :id OR d.id = :id OR d.name LIKE :q OR s.name LIKE :q)
	// 	ORDER BY
	// 		d.name,
	// 		s.name`

	BpmSql = `SELECT reads, bpm_scale_factor FROM bins WHERE size = :bin_size`

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

func (sdb *SeqDB) Dir() string {
	return sdb.url
}

func NewSeqDB(url string) *SeqDB {
	db := sys.Must(sql.Open(sys.Sqlite3DB, filepath.Join(url, "seqs.db"+sys.SqliteReadOnlySuffix)))

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

func (sdb *SeqDB) CanViewSample(sampleId string, isAdmin bool, permissions []string) error {
	namedArgs := []any{sql.Named("id", sampleId)}

	query := sqlite.MakePermissionsSql(CanViewSampleSql, isAdmin, permissions, &namedArgs)

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

// func (sdb *SeqDB) Platforms(assembly string, isAdmin bool, permissions []string) ([]*Platform, error) {
// 	namedArgs := []any{sql.Named("assembly", web.FormatParam(assembly))}

// 	query := sqlite.MakePermissionsSql(TechnologiesSql, isAdmin, permissions, &namedArgs)

// 	rows, err := sdb.db.Query(query, namedArgs...)

// 	if err != nil {
// 		return nil, err //fmt.Errorf("there was an error with the database query")
// 	}

// 	defer rows.Close()

// 	ret := make([]*Platform, 0, 10)

// 	for rows.Next() {
// 		var platform Platform

// 		err := rows.Scan(&platform.Genome,
// 			&platform.Assembly,
// 			&platform.Platform)

// 		if err != nil {
// 			return nil, err //fmt.Errorf("there was an error with the database records")
// 		}

// 		ret = append(ret, &platform)
// 	}

// 	return ret, nil
// }

func (sdb *SeqDB) Datasets(assembly string, isAdmin bool, permissions []string) ([]*Dataset, error) {
	// build sql.Named args
	namedArgs := []any{sql.Named("assembly", web.FormatParam(assembly))}

	query := sqlite.MakePermissionsSql(DatasetsSql, isAdmin, permissions, &namedArgs)

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

// func (sdb *SeqDB) PlatformDatasets(platform string, assembly string, isAdmin bool, permissions []string) ([]*Dataset, error) {
// 	// build sql.Named args

// 	namedArgs := []any{sql.Named("assembly", web.FormatParam(assembly)),
// 		sql.Named("platform", platform)}

// 	query := sqlite.MakePermissionsSql(TechnologyDatasetsSql, isAdmin, permissions, &namedArgs)

// 	// execute query

// 	rows, err := sdb.db.Query(query, namedArgs...)

// 	if err != nil {
// 		return nil, err //fmt.Errorf("there was an error with the database query")
// 	}

// 	defer rows.Close()

// 	ret := make([]*Dataset, 0, 10)

// 	for rows.Next() {
// 		var dataset Dataset

// 		err := rows.Scan(&dataset.Id,
// 			&dataset.Genome,
// 			&dataset.Assembly,
// 			&dataset.Platform,
// 			&dataset.Name)

// 		if err != nil {
// 			return nil, err //fmt.Errorf("there was an error with the database records")
// 		}

// 		ret = append(ret, &dataset)
// 	}

// 	return ret, nil
// }

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

func (sdb *SeqDB) Search(query string, assembly string, isAdmin bool, permissions []string) ([]*Sample, error) {

	var rows *sql.Rows
	var err error

	if query != "" {
		query = strings.TrimSpace(query)

		namedArgs := []any{sql.Named("assembly", web.FormatParam(assembly)),
			sql.Named("id", query),
			sql.Named("q", fmt.Sprintf("%%%s%%", query)),
		}

		query := sqlite.MakePermissionsSql(SearchSamplesSql, isAdmin, permissions, &namedArgs)

		// if platform != "" {
		// 	// platform specific search
		// 	rows, err = sdb.db.Query(SearchPlatformSamplesSql,
		// 		sql.Named("assembly", assembly),
		// 		sql.Named("platform", platform),
		// 		sql.Named("id", query),
		// 		sql.Named("q", fmt.Sprintf("%%%s%%", query)))

		// } else {
		//search all platforms within assembly
		rows, err = sdb.db.Query(query, namedArgs...)
		//}
	} else {
		namedArgs := []any{sql.Named("assembly", web.FormatParam(assembly))}

		query := sqlite.MakePermissionsSql(AllSamplesSql, isAdmin, permissions, &namedArgs)

		rows, err = sdb.db.Query(query, namedArgs...)
	}

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

		// if dataset == nil || dataset.Id != datasetId {
		// 	dataset = &Dataset{
		// 		Id:       datasetId,
		// 		Genome:   genome,
		// 		Assembly: assembly,
		// 		Platform: platform,
		// 		Name:     name,
		// 		Samples:  make([]*Sample, 0, 10),
		// 	}

		// 	datasets = append(datasets, dataset)
		// }

		//sample.Tags = TagsToList(tags)

		//dataset.Samples = append(dataset.Samples, sample)
		ret = append(ret, sample)
	}

	return ret, nil
}

func (sdb *SeqDB) SampleReader(sampleId string, binWidth int) (*SeqReader, error) {

	//const FIND_TRACK_SQL = `SELECT platform, genome, name, reads, stat_mode, url FROM tracks WHERE seq.publicId = ?1`

	row := sdb.db.QueryRow(SampleFromIdSql, sql.Named("id", sampleId))

	sample, err := rowToSample(row)

	if err != nil {
		return nil, err
	}

	url := filepath.Join(sdb.url, sample.Url)

	return NewSeqReader(sample, url, binWidth)
}

type SeqReader struct {
	sample          *Sample
	url             string
	binSize         int
	defaultBinCount int
	//scale           float64
}

func NewSeqReader(sample *Sample, url string, binSize int) (*SeqReader, error) {

	return &SeqReader{
		sample:  sample,
		url:     url,
		binSize: binSize,

		// estimate the number of bins to represent a region
		defaultBinCount: binSize * 4,
	}, nil
}

// func (reader *SeqReader) getPath(location *dna.Location) string {
// 	return filepath.Join(reader.Dir, fmt.Sprintf("bin%d", reader.BinSize), fmt.Sprintf("%s_bin%d_%s.db?mode=ro", location.Chr, reader.BinSize, reader.Track.Genome))
// }

func (reader *SeqReader) BinCounts(location *dna.Location) (*SampleBinCounts, error) {

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
		Bins:    make([]*ReadBin, 0, reader.defaultBinCount),
		YMax:    0,
		BinSize: reader.binSize,
		Reads:   reader.sample.Reads,
	}

	// path := filepath.Join(reader.url,
	// 	fmt.Sprintf("%s.db?mode=ro", location.Chr()))

	path := filepath.Join(reader.url + sys.SqliteReadOnlySuffix)

	log.Debug().Msgf("track path %s", path)

	db, err := sql.Open(sys.Sqlite3DB, path)

	if err != nil {
		return &ret, err
	}

	defer db.Close()

	var bpmReads int
	var scaleFactor float64

	err = db.QueryRow(BpmSql, reader.binSize).Scan(&bpmReads, &scaleFactor) ///endBin)

	if err != nil {
		log.Debug().Msgf("error scale factor %s %s", path, err)
		return &ret, err
	}

	ret.BinReads = bpmReads
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
	var tags string

	err := rows.Scan(&sample.Id,
		&sample.Genome,
		&sample.Assembly,
		&sample.Technology,
		&sample.Dataset,
		&sample.Name,
		&sample.Type,
		&sample.Reads,
		&sample.Url,
		&tags)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database records")
	}

	sample.Tags = TagsToList(tags)

	return &sample, nil
}

func rowToSample(rows *sql.Row) (*Sample, error) {
	var sample Sample
	var tags string

	err := rows.Scan(&sample.Id,
		&sample.Genome,
		&sample.Assembly,
		&sample.Technology,
		&sample.Dataset,
		&sample.Name,
		&sample.Type,
		&sample.Reads,
		&sample.Url,
		&tags)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database records")
	}

	sample.Tags = TagsToList(tags)

	return &sample, nil
}

func TagsToList(tags string) []string {
	tagList := strings.Split(tags, ",")
	sort.Strings(tagList)
	// trim
	for i, tag := range tagList {
		tagList[i] = strings.TrimSpace(tag)
	}

	return tagList
}
