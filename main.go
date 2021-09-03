package goSirene

import (
	"strings"
)

func checkHeader(expected []string, h []string) bool {
	return strings.Join(h, " ") == strings.Join(expected, " ")
}
