package goSirene

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"io"
	"strings"
)

var sireneULHeaders = strings.Split("siren dateFin dateDebut etatAdministratifUniteLegale changementEtatAdministratifUniteLegale nomUniteLegale changementNomUniteLegale nomUsageUniteLegale changementNomUsageUniteLegale denominationUniteLegale changementDenominationUniteLegale denominationUsuelle1UniteLegale denominationUsuelle2UniteLegale denominationUsuelle3UniteLegale changementDenominationUsuelleUniteLegale categorieJuridiqueUniteLegale changementCategorieJuridiqueUniteLegale activitePrincipaleUniteLegale nomenclatureActivitePrincipaleUniteLegale changementActivitePrincipaleUniteLegale nicSiegeUniteLegale changementNicSiegeUniteLegale economieSocialeSolidaireUniteLegale changementEconomieSocialeSolidaireUniteLegale caractereEmployeurUniteLegale changementCaractereEmployeurUniteLegale", " ")

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

func (s *SireneUL) scan(row []string) {
	a := s.toArray()
	for k, v := range row {
		*a[k] = v
	}
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
		if head, err := c.Read(); checkHeader(sireneULHeaders, head) && err != nil {
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

type SireneUL struct {
	err                                           error
	Siren                                         string
	DateFin                                       string
	DateDebut                                     string
	EtatAdministratifUniteLegale                  string
	ChangementEtatAdministratifUniteLegale        string
	NomUniteLegale                                string
	ChangementNomUniteLegale                      string
	NomUsageUniteLegale                           string
	ChangementNomUsageUniteLegale                 string
	DenominationUniteLegale                       string
	ChangementDenominationUniteLegale             string
	DenominationUsuelle1UniteLegale               string
	DenominationUsuelle2UniteLegale               string
	DenominationUsuelle3UniteLegale               string
	ChangementDenominationUsuelleUniteLegale      string
	CategorieJuridiqueUniteLegale                 string
	ChangementCategorieJuridiqueUniteLegale       string
	ActivitePrincipaleUniteLegale                 string
	NomenclatureActivitePrincipaleUniteLegale     string
	ChangementActivitePrincipaleUniteLegale       string
	NicSiegeUniteLegale                           string
	ChangementNicSiegeUniteLegale                 string
	EconomieSocialeSolidaireUniteLegale           string
	ChangementEconomieSocialeSolidaireUniteLegale string
	CaractereEmployeurUniteLegale                 string
	ChangementCaractereEmployeurUniteLegale       string
}

func (s *SireneUL) toArray() []*string {
	return []*string{
		&s.Siren,
		&s.DateFin,
		&s.DateDebut,
		&s.EtatAdministratifUniteLegale,
		&s.ChangementEtatAdministratifUniteLegale,
		&s.NomUniteLegale,
		&s.ChangementNomUniteLegale,
		&s.NomUsageUniteLegale,
		&s.ChangementNomUsageUniteLegale,
		&s.DenominationUniteLegale,
		&s.ChangementDenominationUniteLegale,
		&s.DenominationUsuelle1UniteLegale,
		&s.DenominationUsuelle2UniteLegale,
		&s.DenominationUsuelle3UniteLegale,
		&s.ChangementDenominationUsuelleUniteLegale,
		&s.CategorieJuridiqueUniteLegale,
		&s.ChangementCategorieJuridiqueUniteLegale,
		&s.ActivitePrincipaleUniteLegale,
		&s.NomenclatureActivitePrincipaleUniteLegale,
		&s.ChangementActivitePrincipaleUniteLegale,
		&s.NicSiegeUniteLegale,
		&s.ChangementNicSiegeUniteLegale,
		&s.EconomieSocialeSolidaireUniteLegale,
		&s.ChangementEconomieSocialeSolidaireUniteLegale,
		&s.CaractereEmployeurUniteLegale,
		&s.ChangementCaractereEmployeurUniteLegale,
	}
}
