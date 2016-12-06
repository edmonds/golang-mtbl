package main

import "fmt"
import "log"
import "os"

import "github.com/edmonds/golang-mtbl"

func mergeFunc(key []byte, val0 []byte, val1 []byte) (mergedVal []byte) {
	return []byte(string(val0) + " + " + string(val1))
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <MTBL FILE>\n", os.Args[0])
		os.Exit(1)
	}
	fname := os.Args[1]

	s := mtbl.SorterInit(&mtbl.SorterOptions{Merge: mergeFunc})
	defer s.Destroy()

	w, e := mtbl.WriterInit(fname, &mtbl.WriterOptions{Compression: mtbl.COMPRESSION_SNAPPY})
	defer w.Destroy()
	if e != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", e)
		os.Exit(1)
	}

	for i := 1000; i > 1; i-- {
		key := fmt.Sprintf("Key%0.5d", i)
		val := fmt.Sprintf("Val%0.5d", i)
		e := s.Add([]byte(key), []byte(val))
		if e != nil {
			log.Fatalln(e)
		}
	}

	e = s.Write(w)
	if e != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", e)
		os.Exit(1)
	}
}
