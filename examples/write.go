package main

import "fmt"
import "log"
import "os"

import "mtbl"

func
main() {
    if len(os.Args) != 2 {
        fmt.Fprintf(os.Stderr, "Usage: %s <MTBL FILE>\n", os.Args[0])
        os.Exit(1)
    }
    fname := os.Args[1]

    w, e := mtbl.WriterInit(fname, &mtbl.WriterOptions{Compression: mtbl.COMPRESSION_SNAPPY})
    defer w.Destroy()
    if e != nil {
        fmt.Fprintf(os.Stderr, "Error: %s\n", e)
        os.Exit(1)
    }

    for i := 0; i < 1000; i++ {
        key := fmt.Sprintf("Key%0.5d", i)
        val := fmt.Sprintf("Val%0.5d", i)
        e := w.Add([]byte(key), []byte(val))
        if e != nil {
            log.Fatalln(e)
        }
    }
}
