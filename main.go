package goSirene

import (
	"strings"
)

func checkHeader(expected []string, h []string) bool {
	return strings.Join(h, " ") == strings.Join(expected, " ")
}

func mapHeaders(headers []string) map[string]int {
	m := make(map[string]int)
	for k, v := range headers {
		m[v] = k
	}
	return m
}
