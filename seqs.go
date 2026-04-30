package seqs

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-web"
	"github.com/antonybholmes/go-web/auth/sqlite"
	_ "github.com/mattn/go-sqlite3"
)

type (
	ReadBin struct {
		Start int     `json:"s"`
		End   int     `json:"e"`
		Count float64 `json:"c"`
	}

	SampleBinCounts struct {
		Id string `json:"id"`
		//Name    string     `json:"name"`
		Bins    []*ReadBin `json:"bins"`
		YMax    float64    `json:"ymax"`
		BinSize int        `json:"binSize"`
		//BpmScaleFactor float64    `json:"bpmScaleFactor"`
		Reads int `json:"reads"`

		// sum of all reads falling in all bins, which
		// can be higher than total reads in sample if some reads fall in multiple bins
		BinReads int `json:"binReads"`
	}

	Platform struct {
		Genome   string `json:"genome"`
		Assembly string `json:"assembly"`
		Platform string `json:"platform"`
	}

	Dataset struct {
		Id          string    `json:"id"`
		Genome      string    `json:"genome"`
		Assembly    string    `json:"assembly"`
		Platform    string    `json:"platform"`
		Institution string    `json:"institution"`
		Name        string    `json:"name"`
		Samples     []*Sample `json:"samples"`
	}

	Sample struct {
		Id          string   `json:"id"`
		Genome      string   `json:"genome"`
		Assembly    string   `json:"assembly"`
		Technology  string   `json:"technology"`
		Institution string   `json:"institution"`
		Dataset     string   `json:"dataset"`
		Name        string   `json:"name"`
		Type        string   `json:"type"`
		Url         string   `json:"url"`
		Tags        []string `json:"tags"`
		Reads       int      `json:"reads"`
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
	SampleTypeSeq         = "Seq"
	SampleTypeBigWig      = "Remote BigWig"
	SampleTypeLocalBigWig = "BigWig"

	// TechnologiesSql = `SELECT DISTINCT
	// 	d.public_id,
	// 	g.name as genome,
	// 	a.name as assembly,
	// 	t.name as technology
	// 	FROM datasets d
	// 	JOIN assemblies a ON d.assembly_id = a.id
	// 	JOIN genomes g ON a.genome_id = g.id
	// 	JOIN technologies t ON d.technology_id = t.id
	// 	JOIN dataset_permissions dp ON d.id = dp.dataset_id
	// 	JOIN permissions p ON dp.permission_id = p.id
	// 	WHERE
	// 		<<PERMISSIONS>>
	// 		AND LOWER(a.name) = :assembly
	// 	ORDER BY
	// 		g.name,
	// 		a.name,
	// 		t.name`

	DatasetsSql = `SELECT DISTINCT
		d.public_id,
		g.name AS genome,
		a.name AS assembly,
		ins.name AS institution,
		d.name
		FROM datasets d
		JOIN institutions ins ON d.institution_id = ins.id
		JOIN assemblies a ON d.assembly_id = a.id
		JOIN genomes g ON a.genome_id = g.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			<<PERMISSIONS>>
			AND LOWER(a.name) = :assembly
		ORDER BY
			g.name, 
			a.name,
			ins.name,
			d.name`

	// TechnologyDatasetsSql = `SELECT DISTINCT
	// 	d.public_id,
	// 	a.name as assembly,
	// 	t.name as technology,
	// 	d.name
	// 	FROM datasets d
	// 	JOIN assemblies a ON d.assembly_id = a.id
	// 	JOIN technologies t ON d.technology_id = t.id
	// 	JOIN dataset_permissions dp ON d.id = dp.dataset_id
	// 	JOIN permissions p ON dp.permission_id = p.id
	// 	WHERE
	// 		<<PERMISSIONS>>
	// 		AND t.name = :technology
	// 		AND LOWER(a.name) = :assembly
	// 	ORDER BY
	// 		a.name,
	// 		d.name`

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
		ins.name AS institution,
		d.name AS dataset_name,
		s.name AS sample_name,
		st.name AS sample_type, 
		s.reads,
		s.url, 
		s.tags
		FROM samples s
		JOIN datasets d ON s.dataset_id = d.id
		JOIN institutions ins ON d.institution_id = ins.id
		JOIN assemblies a ON d.assembly_id = a.id
		JOIN genomes g ON a.genome_id = g.id
		JOIN technologies t ON s.technology_id = t.id
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
			ins.name,
			d.name, 
			s.name`

	SearchSamplesSql = BaseSearchSamplesSql +
		` AND (
			s.public_id = :id 
			OR d.public_id = :id 
			OR LOWER(t.name) = :id 
			OR d.name LIKE :q 
			OR s.name LIKE :q)
		ORDER BY 
			t.name,
			ins.name,
			d.name, 
			s.name`
)

func (sdb *SeqDB) Dir() string {
	return sdb.url
}

func NewSeqDB(url string) *SeqDB {
	db := sys.Must(sql.Open(sys.Sqlite3DB, filepath.Join(url, "seqs.db"+sys.SqliteDSN)))

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
			&dataset.Institution,
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

type SeqReader interface {
	BinCounts(location *dna.Location) (*SampleBinCounts, error)
}

func (sdb *SeqDB) ReaderFromId(sampleId string, binWidth int) (SeqReader, error) {

	//const FIND_TRACK_SQL = `SELECT platform, genome, name, reads, stat_mode, url FROM tracks WHERE seq.publicId = ?1`

	row := sdb.db.QueryRow(SampleFromIdSql, sql.Named("id", sampleId))

	sample, err := rowToSample(row)

	if err != nil {
		return nil, err
	}

	url := filepath.Join(sdb.url, sample.Url)

	switch sample.Type {
	case SampleTypeBigWig, SampleTypeLocalBigWig:
		return NewBigWigReader(sample, url, binWidth)
	default:
		return NewDBSeqReader(sample, url, binWidth)
	}

}
