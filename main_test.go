package goSirene

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/cnf/structhash"
)

var geoSireneFile = "test/StockEtablissement_utf8_geo.csv.gz"
var sireneULZIPFile = "test/StockUniteLegale_utf8.zip"
var sireneULPlainFile = "test/StockUniteLegale_utf8.csv"

func Test_GeoSireneReader(t *testing.T) {
	t.Log("Test GeoSirene Reader with real data")
	file, err := os.Open(geoSireneFile)

	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := GeoSireneParser(ctx, file)
	result := []GeoSirene{}
	for sirene := range s {
		if sirene.err == nil {
			result = append(result, sirene)
		} else {
			t.Log("Error encountered when reading data: " + sirene.err.Error())
			t.Fatal()
		}
	}
	md5 := fmt.Sprintf("%x", structhash.Md5(result, 1))
	expected := "6f1ac5ec70776c9607702bfb70f1d2c9"
	if md5 != expected {
		t.Logf("hash should be %s, is %s", expected, md5)
		t.Fail()
	}
}

func Test_SireneULZIPReader(t *testing.T) {
	ctx := context.Background()
	s := SireneULParser(ctx, sireneULZIPFile)
	result := []SireneUL{}
	for sirene := range s {
		if sirene.err == nil {
			result = append(result, sirene)
		} else {
			t.Log("Error encountered when reading data: " + sirene.err.Error())
			t.Fatal()
		}
	}
	md5 := fmt.Sprintf("%x", structhash.Md5(result, 1))
	expected := "5efa2b5264247e1f5123f601055434e5"
	if md5 != expected {
		t.Logf("hash should be %s, is %s", expected, md5)
		t.Fail()
	}
}

func Test_SireneULPlainReader(t *testing.T) {
	ctx := context.Background()
	s := SireneULParser(ctx, sireneULPlainFile)
	result := []SireneUL{}
	for sirene := range s {
		if sirene.err == nil {
			result = append(result, sirene)
		} else {
			t.Log("Error encountered when reading data: " + sirene.err.Error())
			t.Fatal()
		}
	}
	md5 := fmt.Sprintf("%x", structhash.Md5(result, 1))
	expected := "5efa2b5264247e1f5123f601055434e5"
	if md5 != expected {
		t.Logf("hash should be %s, is %s", expected, md5)
		t.Fail()
	}
}

func Test_GeoSireneReaderCancelContext(t *testing.T) {
	t.Log("Test Reader with real data")
	file, err := os.Open(geoSireneFile)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ctx, fn := context.WithCancel(ctx)
	s := GeoSireneParser(ctx, file)
	v, ok := <-s
	if !ok {
		t.Log("Channel doesn't work")
	}
	if v.err != nil {
		t.Log("Error encountered when reading data: " + v.err.Error())
		t.Fatal()
	}
	fn()
	_, ok = <-s
	if ok {
		t.Log("Channel is open but should be closed")
		t.Fatal()
	}
}

func Test_SireneULReaderCancelContext(t *testing.T) {
	t.Log("Test Reader with real data")

	ctx := context.Background()
	ctx, fn := context.WithCancel(ctx)
	s := SireneULParser(ctx, sireneULZIPFile)
	v, ok := <-s
	if !ok {
		t.Log("Channel doesn't work")
		t.Fatal()
	}
	if v.err != nil {
		t.Log("Error encountered when reading data: " + v.err.Error())
		t.Fatal()
	}
	fn()
	_, ok = <-s
	if ok {
		t.Log("Channel is open but should be closed")
		t.Fatal()
	}
}
