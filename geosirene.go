package goSirene

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"io"
	"strconv"
	"strings"
	"time"
)

var GeoSireneHeaders = strings.Split("siren nic siret statutDiffusionEtablissement dateCreationEtablissement trancheEffectifsEtablissement anneeEffectifsEtablissement activitePrincipaleRegistreMetiersEtablissement dateDernierTraitementEtablissement etablissementSiege nombrePeriodesEtablissement complementAdresseEtablissement numeroVoieEtablissement indiceRepetitionEtablissement typeVoieEtablissement libelleVoieEtablissement codePostalEtablissement libelleCommuneEtablissement libelleCommuneEtrangerEtablissement distributionSpecialeEtablissement codeCommuneEtablissement codeCedexEtablissement libelleCedexEtablissement codePaysEtrangerEtablissement libellePaysEtrangerEtablissement complementAdresse2Etablissement numeroVoie2Etablissement indiceRepetition2Etablissement typeVoie2Etablissement libelleVoie2Etablissement codePostal2Etablissement libelleCommune2Etablissement libelleCommuneEtranger2Etablissement distributionSpeciale2Etablissement codeCommune2Etablissement codeCedex2Etablissement libelleCedex2Etablissement codePaysEtranger2Etablissement libellePaysEtranger2Etablissement dateDebut etatAdministratifEtablissement enseigne1Etablissement enseigne2Etablissement enseigne3Etablissement denominationUsuelleEtablissement activitePrincipaleEtablissement nomenclatureActivitePrincipaleEtablissement caractereEmployeurEtablissement longitude latitude geo_score geo_type geo_adresse geo_id geo_ligne geo_l4 geo_l5", " ")
var GeoSireneMap = mapHeaders(GeoSireneHeaders)

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
	defer close(s)
	defer r.Close()
	if err != nil {
		s <- GeoSirene{err: err}
		return
	}
	defer gzr.Close()

	c := csv.NewReader(gzr)
	if head, err := c.Read(); checkHeader(GeoSireneHeaders, head) && err != nil {
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
			s <- sirene
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
	err                                            error
	Siren                                          string
	Nic                                            string
	Siret                                          string
	StatutDiffusionEtablissement                   string
	DateCreationEtablissement                      time.Time
	TrancheEffectifsEtablissement                  string
	AnneeEffectifsEtablissement                    string
	ActivitePrincipaleRegistreMetiersEtablissement string
	DateDernierTraitementEtablissement             time.Time
	EtablissementSiege                             bool
	NombrePeriodesEtablissement                    int
	ComplementAdresseEtablissement                 string
	NumeroVoieEtablissement                        string
	IndiceRepetitionEtablissement                  string
	TypeVoieEtablissement                          string
	LibelleVoieEtablissement                       string
	CodePostalEtablissement                        string
	LibelleCommuneEtablissement                    string
	LibelleCommuneEtrangerEtablissement            string
	DistributionSpecialeEtablissement              string
	CodeCommuneEtablissement                       string
	CodeCedexEtablissement                         string
	LibelleCedexEtablissement                      string
	CodePaysEtrangerEtablissement                  string
	LibellePaysEtrangerEtablissement               string
	ComplementAdresse2Etablissement                string
	NumeroVoie2Etablissement                       string
	IndiceRepetition2Etablissement                 string
	TypeVoie2Etablissement                         string
	LibelleVoie2Etablissement                      string
	CodePostal2Etablissement                       string
	LibelleCommune2Etablissement                   string
	LibelleCommuneEtranger2Etablissement           string
	DistributionSpeciale2Etablissement             string
	CodeCommune2Etablissement                      string
	CodeCedex2Etablissement                        string
	LibelleCedex2Etablissement                     string
	CodePaysEtranger2Etablissement                 string
	LibellePaysEtranger2Etablissement              string
	DateDebut                                      time.Time
	EtatAdministratifEtablissement                 string
	Enseigne1Etablissement                         string
	Enseigne2Etablissement                         string
	Enseigne3Etablissement                         string
	DenominationUsuelleEtablissement               string
	ActivitePrincipaleEtablissement                string
	NomenclatureActivitePrincipaleEtablissement    string
	CaractereEmployeurEtablissement                bool
	Longitude                                      float64
	Latitude                                       float64
	Geo_score                                      float64
	Geo_type                                       string
	Geo_adresse                                    string
	Geo_id                                         string
	Geo_ligne                                      string
	Geo_l4                                         string
	Geo_l5                                         string
}

func (s GeoSirene) Error() error {
	return s.err
}

func (s *GeoSirene) scan(row []string) {
	s.Siren = row[0]
	s.Nic = row[1]
	s.Siret = row[2]
	s.StatutDiffusionEtablissement = row[3]
	s.DateCreationEtablissement, _ = time.Parse("2006-01-02", row[4])
	s.TrancheEffectifsEtablissement = row[5]
	s.AnneeEffectifsEtablissement = row[6]
	s.ActivitePrincipaleRegistreMetiersEtablissement = row[7]
	s.DateDernierTraitementEtablissement, _ = time.Parse("2006-01-02T15:04:05", row[8])
	s.EtablissementSiege, _ = strconv.ParseBool(row[9])
	s.NombrePeriodesEtablissement, _ = strconv.Atoi(row[10])
	s.ComplementAdresseEtablissement = row[11]
	s.NumeroVoieEtablissement = row[12]
	s.IndiceRepetitionEtablissement = row[13]
	// Skipping dernierNumeroVoieEtablissement, indiceRepetitionDernierNumeroVoieEtablissement part of the new insee format from March 2024
	s.TypeVoieEtablissement = row[16]
	s.LibelleVoieEtablissement = row[17]
	s.CodePostalEtablissement = row[18]
	s.LibelleCommuneEtablissement = row[19]
	s.LibelleCommuneEtrangerEtablissement = row[20]
	s.DistributionSpecialeEtablissement = row[21]
	s.CodeCommuneEtablissement = row[22]
	s.CodeCedexEtablissement = row[23]
	s.LibelleCedexEtablissement = row[24]
	s.CodePaysEtrangerEtablissement = row[25]
	s.LibellePaysEtrangerEtablissement = row[26]
	// Skipping identifiantAdresseEtablissement, coordonneeLambertAbscisseEtablissement and coordonneeLambertOrdonneeEtablissement part of the new insee format from March 2024
	s.ComplementAdresse2Etablissement = row[30]
	s.NumeroVoie2Etablissement = row[31]
	s.IndiceRepetition2Etablissement = row[32]
	s.TypeVoie2Etablissement = row[33]
	s.LibelleVoie2Etablissement = row[34]
	s.CodePostal2Etablissement = row[35]
	s.LibelleCommune2Etablissement = row[36]
	s.LibelleCommuneEtranger2Etablissement = row[37]
	s.DistributionSpeciale2Etablissement = row[38]
	s.CodeCommune2Etablissement = row[39]
	s.CodeCedex2Etablissement = row[40]
	s.LibelleCedex2Etablissement = row[41]
	s.CodePaysEtranger2Etablissement = row[42]
	s.LibellePaysEtranger2Etablissement = row[43]
	s.DateDebut, _ = time.Parse("2006-01-02", row[44])
	s.EtatAdministratifEtablissement = row[45]
	s.Enseigne1Etablissement = row[46]
	s.Enseigne2Etablissement = row[47]
	s.Enseigne3Etablissement = row[48]
	s.DenominationUsuelleEtablissement = row[49]
	s.ActivitePrincipaleEtablissement = row[50]
	s.NomenclatureActivitePrincipaleEtablissement = row[51]
	s.CaractereEmployeurEtablissement = row[52] == "O"
	s.Longitude, _ = strconv.ParseFloat(row[53], 64)
	s.Latitude, _ = strconv.ParseFloat(row[54], 64)
	s.Geo_score, _ = strconv.ParseFloat(row[55], 64)
	s.Geo_type = row[56]
	s.Geo_adresse = row[57]
	s.Geo_id = row[58]
	s.Geo_ligne = row[59]
	s.Geo_l4 = row[60]
	s.Geo_l5 = row[61]
}
