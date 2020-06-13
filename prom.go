package main

import (
	"log"
	"os"
	"path"
)

type PrometheusExpoWriter struct {
	fp string
}

func NewPrometheusExportWriter(dir, tag string) *PrometheusExpoWriter {
	var logFilePath = path.Join(dir, "stats-"+tag+".prom")
	return &PrometheusExpoWriter{
		fp: logFilePath, // "/run/jlogger/stats.prom"
	}
}

// Write implements the writer interface and allows for implementing a customer
// writer, which encapsulates writing to a file and additional file management
// operations, such as creating a temporary file, etc.
func (s *PrometheusExpoWriter) Write(data []byte) (n int, err error) {
	var f *os.File
	var permFilename = s.fp
	var tmpFilename = permFilename + ".t"

	if f, err = os.OpenFile(
		tmpFilename,
		os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return 0, err
	} else {
		defer func() {
			f.Close()
			// To avoid partially written files during scrapes we write the file
			// out with a temporary name and then rename it to desired name.
			if err = os.Rename(tmpFilename, permFilename); err != nil {
				log.Panic(err)
			}
		}()
	}
	return f.Write(data)
}
