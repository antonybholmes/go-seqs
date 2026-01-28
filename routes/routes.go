package routes

import (
	"errors"

	"github.com/antonybholmes/go-dna"
	seq "github.com/antonybholmes/go-seqs"
	"github.com/antonybholmes/go-seqs/seqdb"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-web"
	"github.com/antonybholmes/go-web/middleware"
	"github.com/gin-gonic/gin"
)

var (
	ErrNoGenomeSupplied = errors.New("must supply a genome")
)

type (
	ReqSeqParams struct {
		Locations []string `json:"locations"`
		Scale     float64  `json:"scale"`
		BinSizes  []int    `json:"binSizes"`
		Samples   []string `json:"samples"`
	}

	SeqParams struct {
		Locations []*dna.Location
		Scale     float64
		BinSizes  []int
		Samples   []string
	}

	SeqResp struct {
		Location *dna.Location          `json:"location"`
		Samples  []*seq.SampleBinCounts `json:"samples"`
	}
)

func ParseSeqParamsFromPost(c *gin.Context) (*SeqParams, error) {

	var params ReqSeqParams

	err := c.Bind(&params)

	if err != nil {
		return nil, err
	}

	locations := make([]*dna.Location, 0, len(params.Locations))

	for _, loc := range params.Locations {
		location, err := dna.ParseLocation(loc)

		if err != nil {
			return nil, err
		}

		locations = append(locations, location)
	}

	return &SeqParams{
			Locations: locations,
			BinSizes:  params.BinSizes,
			Samples:   params.Samples,
			Scale:     params.Scale},
		nil
}

// func GenomesRoute(c *gin.Context) {
// 	user, err := middleware.GetJwtUser(c)

// 	if err != nil {
// 		c.Error(err)
// 		return
// 	}

// 	platforms, err := seqdb.Genomes(user.Permissions)

// 	if err != nil {
// 		c.Error(err)
// 		return
// 	}

// 	web.MakeDataResp(c, "", platforms)
// }

func PlatformsRoute(c *gin.Context) {
	user, err := middleware.GetJwtUser(c)

	if err != nil {
		c.Error(err)
		return
	}

	assembly := c.Param("assembly")

	platforms, err := seqdb.Platforms(assembly, user.Permissions)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", platforms)
}

func PlatformDatasetsRoute(c *gin.Context) {
	user, err := middleware.GetJwtUser(c)

	if err != nil {
		c.Error(err)
		return
	}

	platform := c.Param("platform")
	assembly := c.Param("assembly")

	tracks, err := seqdb.PlatformDatasets(platform, assembly, user.Permissions)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", tracks)
}

func SearchSeqRoute(c *gin.Context) {
	user, err := middleware.GetJwtUser(c)

	if err != nil {
		c.Error(err)
		return
	}

	assembly := c.Param("assembly")

	if assembly == "" {
		web.BadReqResp(c, ErrNoGenomeSupplied)
		return
	}

	query := c.Query("search")

	tracks, err := seqdb.Search(assembly, query, user.Permissions)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", tracks)
}

func BinsRoute(c *gin.Context) {

	user, err := middleware.GetJwtUser(c)

	if err != nil {
		c.Error(err)
		return
	}

	params, err := ParseSeqParamsFromPost(c)

	if err != nil {
		log.Debug().Msgf("err %s", err)
		c.Error(err)
		return
	}

	//log.Debug().Msgf("bin %v %v", params.Locations, params.BinSizes)

	ret := make([]*SeqResp, 0, len(params.Locations)) //make([]*seq.BinCounts, 0, len(params.Tracks))

	for li, location := range params.Locations {
		resp := SeqResp{Location: location, Samples: make([]*seq.SampleBinCounts, 0, len(params.Samples))}

		for _, sample := range params.Samples {
			err := seqdb.CanViewSample(sample, user.Permissions)

			if err != nil {
				//log.Debug().Msgf("no permission for sample %s: %s", sample, err)
				continue
			}

			reader, err := seqdb.ReaderFromId(sample,
				params.BinSizes[li],
				params.Scale)

			if err != nil {
				//log.Debug().Msgf("stupid err %s", err)
				c.Error(err)
				return
			}

			// guarantees something is returned even with error
			// so we can ignore the errors for now to make the api
			// more robus
			sampleBinCounts, _ := reader.SampleBinCounts(location)

			// if err != nil {
			// 	return web.ErrorReq(err)
			// }

			resp.Samples = append(resp.Samples, sampleBinCounts)
		}

		ret = append(ret, &resp)
	}

	//log.Debug().Msgf("ret %v", len(ret))

	web.MakeDataResp(c, "", ret)
}
