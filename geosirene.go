package goSirene

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"io"
	"strings"
)

var geoSireneHeaders = strings.Split("siren nic l1_normalisee l2_normalisee l3_normalisee l4_normalisee l5_normalisee l6_normalisee l7_normalisee l1_declaree l2_declaree l3_declareeee l4_declaree l5_declaree l6_declaree l7_declaree numvoie indrep typvoie libvoie codpos cedex rpet libreg depet arronet ctonet comet libcom du tu uu epci tcd zemet siege enseigne ind_publipo diffcom amintret natetab libnatetab apet700 libapet dapet tefet libtefet efetcent defet origine dcret ddebact activnat lieuact actisurf saisonat modet prodet prodpart auxilt nomen_long sigle nom prenom civilite rna nicsiege rpen depcomen adr_mail nj libnj apen700 libapen dapen aprm ess dateess tefen libtefen efencent defen categorie dcren amintren monoact moden proden esaann tca esaapen esasec1n esasec2n esasec3n esasec4n vmaj vmaj1 vmaj2 vmaj3 datemaj latitude longitude geo_score geo_type geo_adresse geo_id geo_ligne geo_l4 geo_l5", " ")

func geoSireneFromCsv(row []string) GeoSirene {
	s := GeoSirene{}
	s.scan(row)
	return s
}

// GeoSireneParses returns a GeoSirene channel.
// Errors are transmitted trough GeoSirene.Error() function.
func GeoSireneParser(ctx context.Context, r io.ReadCloser) chan GeoSirene {
	s := make(chan GeoSirene)
	go readGeoSirene(ctx, r, s)
	return s
}

func readGeoSirene(ctx context.Context, r io.ReadCloser, s chan GeoSirene) {
	gzr, err := gzip.NewReader(r)
	if err != nil {
		s <- GeoSirene{err: err}
	}

	defer close(s)
	defer gzr.Close()
	defer r.Close()

	c := csv.NewReader(gzr)
	if head, err := c.Read(); checkHeader(geoSireneHeaders, head) && err != nil {
		s <- GeoSirene{err: err}
		return
	}
	for {
		row, err := c.Read()
		if err != nil {
			if err == io.EOF {
				return
			}
			s <- GeoSirene{err: err}
			return
		}

		sirene := geoSireneFromCsv(row)
		if sirene.err != nil {
			return
		}

		select {
		case <-ctx.Done():
			return // avoid leaking of this goroutine when ctx is done.
		case s <- sirene:
			continue
		}
	}
}

type GeoSirene struct {
	err           error
	Siren         string
	Nic           string
	L1_normalisee string
	L2_normalisee string
	L3_normalisee string
	L4_normalisee string
	L5_normalisee string
	L6_normalisee string
	L7_normalisee string
	L1_declaree   string
	L2_declaree   string
	L3_declareeee string
	L4_declaree   string
	L5_declaree   string
	L6_declaree   string
	L7_declaree   string
	Numvoie       string
	Indrep        string
	Typvoie       string
	Libvoie       string
	Codpos        string
	Cedex         string
	Rpet          string
	Libreg        string
	Depet         string
	Arronet       string
	Ctonet        string
	Comet         string
	Libcom        string
	Du            string
	Tu            string
	Uu            string
	Epci          string
	Tcd           string
	Zemet         string
	Siege         string
	Enseigne      string
	Ind_publipo   string
	Diffcom       string
	Amintret      string
	Natetab       string
	Libnatetab    string
	Apet700       string
	Libapet       string
	Dapet         string
	Tefet         string
	Libtefet      string
	Efetcent      string
	Defet         string
	Origine       string
	Dcret         string
	Ddebact       string
	Activnat      string
	Lieuact       string
	Actisurf      string
	Saisonat      string
	Modet         string
	Prodet        string
	Prodpart      string
	Auxilt        string
	Nomen_long    string
	Sigle         string
	Nom           string
	Prenom        string
	Civilite      string
	Rna           string
	Nicsiege      string
	Rpen          string
	Depcomen      string
	Adr_mail      string
	Nj            string
	Libnj         string
	Apen700       string
	Libapen       string
	Dapen         string
	Aprm          string
	Ess           string
	Dateess       string
	Tefen         string
	Libtefen      string
	Efencent      string
	Defen         string
	Categorie     string
	Dcren         string
	Amintren      string
	Monoact       string
	Moden         string
	Proden        string
	Esaann        string
	Tca           string
	Esaapen       string
	Esasec1n      string
	Esasec2n      string
	Esasec3n      string
	Esasec4n      string
	Vmaj          string
	Vmaj1         string
	Vmaj2         string
	Vmaj3         string
	Datemaj       string
	Latitude      string
	Longitude     string
	Geo_score     string
	Geo_type      string
	Geo_adresse   string
	Geo_id        string
	Geo_ligne     string
	Geo_l4        string
	Geo_l5        string
}

func (s GeoSirene) Error() error {
	return s.err
}

func (s *GeoSirene) scan(row []string) {
	a := s.toArray()
	for k, v := range row {
		*a[k] = v
	}
}

func (s *GeoSirene) toArray() []*string {
	return []*string{
		&s.Siren,
		&s.Nic,
		&s.L1_normalisee,
		&s.L2_normalisee,
		&s.L3_normalisee,
		&s.L4_normalisee,
		&s.L5_normalisee,
		&s.L6_normalisee,
		&s.L7_normalisee,
		&s.L1_declaree,
		&s.L2_declaree,
		&s.L3_declareeee,
		&s.L4_declaree,
		&s.L5_declaree,
		&s.L6_declaree,
		&s.L7_declaree,
		&s.Numvoie,
		&s.Indrep,
		&s.Typvoie,
		&s.Libvoie,
		&s.Codpos,
		&s.Cedex,
		&s.Rpet,
		&s.Libreg,
		&s.Depet,
		&s.Arronet,
		&s.Ctonet,
		&s.Comet,
		&s.Libcom,
		&s.Du,
		&s.Tu,
		&s.Uu,
		&s.Epci,
		&s.Tcd,
		&s.Zemet,
		&s.Siege,
		&s.Enseigne,
		&s.Ind_publipo,
		&s.Diffcom,
		&s.Amintret,
		&s.Natetab,
		&s.Libnatetab,
		&s.Apet700,
		&s.Libapet,
		&s.Dapet,
		&s.Tefet,
		&s.Libtefet,
		&s.Efetcent,
		&s.Defet,
		&s.Origine,
		&s.Dcret,
		&s.Ddebact,
		&s.Activnat,
		&s.Lieuact,
		&s.Actisurf,
		&s.Saisonat,
		&s.Modet,
		&s.Prodet,
		&s.Prodpart,
		&s.Auxilt,
		&s.Nomen_long,
		&s.Sigle,
		&s.Nom,
		&s.Prenom,
		&s.Civilite,
		&s.Rna,
		&s.Nicsiege,
		&s.Rpen,
		&s.Depcomen,
		&s.Adr_mail,
		&s.Nj,
		&s.Libnj,
		&s.Apen700,
		&s.Libapen,
		&s.Dapen,
		&s.Aprm,
		&s.Ess,
		&s.Dateess,
		&s.Tefen,
		&s.Libtefen,
		&s.Efencent,
		&s.Defen,
		&s.Categorie,
		&s.Dcren,
		&s.Amintren,
		&s.Monoact,
		&s.Moden,
		&s.Proden,
		&s.Esaann,
		&s.Tca,
		&s.Esaapen,
		&s.Esasec1n,
		&s.Esasec2n,
		&s.Esasec3n,
		&s.Esasec4n,
		&s.Vmaj,
		&s.Vmaj1,
		&s.Vmaj2,
		&s.Vmaj3,
		&s.Datemaj,
		&s.Latitude,
		&s.Longitude,
		&s.Geo_score,
		&s.Geo_type,
		&s.Geo_adresse,
		&s.Geo_id,
		&s.Geo_ligne,
		&s.Geo_l4,
		&s.Geo_l5,
	}
}
