package goSirene

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"io"
	"strconv"
	"strings"
	"time"
)

var SireneULHeaders = strings.Split("siren statutDiffusionUniteLegale unitePurgeeUniteLegale dateCreationUniteLegale sigleUniteLegale sexeUniteLegale prenom1UniteLegale prenom2UniteLegale prenom3UniteLegale prenom4UniteLegale prenomUsuelUniteLegale pseudonymeUniteLegale identifiantAssociationUniteLegale trancheEffectifsUniteLegale anneeEffectifsUniteLegale dateDernierTraitementUniteLegale nombrePeriodesUniteLegale categorieEntreprise anneeCategorieEntreprise dateDebut etatAdministratifUniteLegale nomUniteLegale nomUsageUniteLegale denominationUniteLegale denominationUsuelle1UniteLegale denominationUsuelle2UniteLegale denominationUsuelle3UniteLegale categorieJuridiqueUniteLegale activitePrincipaleUniteLegale nomenclatureActivitePrincipaleUniteLegale nicSiegeUniteLegale economieSocialeSolidaireUniteLegale caractereEmployeurUniteLegale", " ")
var SireneULMap = mapHeaders(SireneULHeaders)

// GeoSireneParses returns a GeoSirene channel.
// Errors are transmitted trough GeoSirene.Error() function.
func SireneULParser(ctx context.Context, path string) chan SireneUL {
	s := make(chan SireneUL)
	go readSireneUL(ctx, path, s)
	return s
}

func sireneULFromCsv(row []string) SireneUL {
	s := SireneUL{}
	s.scan(row)
	return s
}

func readSireneUL(ctx context.Context, path string, s chan SireneUL) {
	zr, err := zip.OpenReader(path)
	if err != nil {
		s <- SireneUL{err: err}
		return
	}

	defer close(s)
	defer zr.Close()

	for _, zf := range zr.File {
		f, err := zf.Open()
		if err != nil {
			s <- SireneUL{err: err}
			return
		}
		c := csv.NewReader(f)
		if head, err := c.Read(); checkHeader(SireneULHeaders, head) && err != nil {
			s <- SireneUL{err: err}
			return
		}
		for {
			row, err := c.Read()
			if err != nil {
				if err == io.EOF {
					err := f.Close()
					if err != nil {
						s <- SireneUL{err: err}
					}
					return
				}
				f.Close()
				s <- SireneUL{err: err}
				return
			}
			sirene := sireneULFromCsv(row)
			if sirene.err != nil {
				return
			}

			select {
			case <-ctx.Done():
				return
			case s <- sirene:
				continue
			}
		}
	}
}

func (s *SireneUL) scan(row []string) {
	s.Siren = row[0]
	s.StatutDiffusionUniteLegale = row[1] == "0"
	s.UnitePurgeeUniteLegale = row[2] == "true"
	s.DateCreationUniteLegale, _ = time.Parse("2006-01-02", row[3])
	s.SigleUniteLegale = row[4]
	s.SexeUniteLegale = row[5]
	s.Prenom1UniteLegale = row[6]
	s.Prenom2UniteLegale = row[7]
	s.Prenom3UniteLegale = row[8]
	s.Prenom4UniteLegale = row[9]
	s.PrenomUsuelUniteLegale = row[10]
	s.PseudonymeUniteLegale = row[11]
	s.IdentifiantAssociationUniteLegale = row[12]
	s.TrancheEffectifsUniteLegale = row[13]
	s.AnneeEffectifsUniteLegale = row[14]
	s.DateDernierTraitementUniteLegale, _ = time.Parse("2006-01-02", row[15])
	s.NombrePeriodesUniteLegale, _ = strconv.Atoi(row[16])
	s.CategorieEntreprise = row[17]
	s.AnneeCategorieEntreprise = row[18]
	s.DateDebut, _ = time.Parse("2006-01-02", row[19])
	s.EtatAdministratifUniteLegale = row[20]
	s.NomUniteLegale = row[21]
	s.NomUsageUniteLegale = row[22]
	s.DenominationUniteLegale = row[23]
	s.DenominationUsuelle1UniteLegale = row[24]
	s.DenominationUsuelle2UniteLegale = row[25]
	s.DenominationUsuelle3UniteLegale = row[26]
	s.CategorieJuridiqueUniteLegale = row[27]
	s.ActivitePrincipaleUniteLegale = row[28]
	s.NomenclatureActivitePrincipaleUniteLegale = row[29]
	s.NicSiegeUniteLegale = row[30]
	s.EconomieSocialeSolidaireUniteLegale = row[31] == "O"
	s.CaractereEmployeurUniteLegale = row[32] == "O"
}

type SireneUL struct {
	err                                       error
	Siren                                     string
	StatutDiffusionUniteLegale                bool
	UnitePurgeeUniteLegale                    bool
	DateCreationUniteLegale                   time.Time
	SigleUniteLegale                          string
	SexeUniteLegale                           string
	Prenom1UniteLegale                        string
	Prenom2UniteLegale                        string
	Prenom3UniteLegale                        string
	Prenom4UniteLegale                        string
	PrenomUsuelUniteLegale                    string
	PseudonymeUniteLegale                     string
	IdentifiantAssociationUniteLegale         string
	TrancheEffectifsUniteLegale               string
	AnneeEffectifsUniteLegale                 string
	DateDernierTraitementUniteLegale          time.Time
	NombrePeriodesUniteLegale                 int
	CategorieEntreprise                       string
	AnneeCategorieEntreprise                  string
	DateDebut                                 time.Time
	EtatAdministratifUniteLegale              string
	NomUniteLegale                            string
	NomUsageUniteLegale                       string
	DenominationUniteLegale                   string
	DenominationUsuelle1UniteLegale           string
	DenominationUsuelle2UniteLegale           string
	DenominationUsuelle3UniteLegale           string
	CategorieJuridiqueUniteLegale             string
	ActivitePrincipaleUniteLegale             string
	NomenclatureActivitePrincipaleUniteLegale string
	NicSiegeUniteLegale                       string
	EconomieSocialeSolidaireUniteLegale       bool
	CaractereEmployeurUniteLegale             bool
}

// RaisonSociale produit la Raison Sociale à partir des champs de l'unité légale
func (s SireneUL) RaisonSociale() string {
	var nomUsageUniteLegale string

	if s.NomUsageUniteLegale != "" {
		nomUsageUniteLegale = s.NomUsageUniteLegale + "/"
	}

	return s.DenominationUniteLegale +
		strings.Trim(s.NomUniteLegale+"*"+
			nomUsageUniteLegale+
			s.Prenom1UniteLegale+" "+
			s.Prenom2UniteLegale+" "+
			s.Prenom3UniteLegale+" "+
			s.Prenom4UniteLegale+" ",
			" ") + "/"
}
