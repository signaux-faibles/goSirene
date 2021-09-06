package goSirene

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/cnf/structhash"
)

func Test_GeoSireneReader(t *testing.T) {
	t.Log("Test GeoSirene Reader with real data")
	file, err := os.Open("test/etablissements_actifs.csv.gz")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s := GeoSireneParser(ctx, file)
	result := []GeoSirene{}
	for sirene := range s {
		result = append(result, sirene)
	}
	md5 := fmt.Sprintf("%x", structhash.Md5(result, 1))
	expected := "04c8544c21a6e5b28491c1cf37e21879"
	if md5 != expected {
		t.Logf("hash should be %s, is %s", expected, md5)
		t.Fail()
	}
}

func Test_SireneULReader(t *testing.T) {
	ctx := context.Background()
	s := SireneULParser(ctx, "test/sireneUL.zip")
	result := []SireneUL{}
	for sirene := range s {
		result = append(result, sirene)
	}
	md5 := fmt.Sprintf("%x", structhash.Md5(result, 1))
	expected := "873f2f701e68efd717523b99e99c4c7b"
	if md5 != expected {
		t.Logf("hash should be %s, is %s", expected, md5)
		t.Fail()
	}
}
func Test_GeoSireneReaderCancelContext(t *testing.T) {
	t.Log("Test Reader with real data")
	file, err := os.Open("test/etablissements_actifs.csv.gz")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ctx, fn := context.WithCancel(ctx)
	s := GeoSireneParser(ctx, file)
	_, ok := <-s
	if !ok {
		t.Log("Channel doesn't work")
		t.Fail()
	}
	fn()
	_, ok = <-s
	if ok {
		t.Log("Channel is open but should be closed")
		t.Fail()
	}
}

func Test_SireneULReaderCancelContext(t *testing.T) {
	t.Log("Test Reader with real data")

	ctx := context.Background()
	ctx, fn := context.WithCancel(ctx)
	s := SireneULParser(ctx, "test/sireneUL.zip")
	_, ok := <-s
	if !ok {
		t.Log("Channel doesn't work")
		t.Fail()
	}
	fn()
	_, ok = <-s
	if ok {
		t.Log("Channel is open but should be closed")
		t.Fail()
	}
}
