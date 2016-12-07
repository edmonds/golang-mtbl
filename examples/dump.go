package main

import "fmt"
import "os"

import "github.com/edmonds/golang-mtbl"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <MTBL FILE>\n", os.Args[0])
		os.Exit(1)
	}
	fname := os.Args[1]

	r, e := mtbl.ReaderInit(fname, &mtbl.ReaderOptions{VerifyChecksums: true})
	defer r.Destroy()
	if e != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", e)
		os.Exit(1)
	}

	it := mtbl.IterAll(r)
	defer it.Destroy()
	for {
		key, val, ok := it.Next()
		if !ok {
			break
		}
		fmt.Printf("%q %q\n", key, val)
	}
}
