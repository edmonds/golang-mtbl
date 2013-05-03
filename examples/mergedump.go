package main

import "fmt"
import "os"

import "mtbl"

func mergeFunc(key []byte, val0 []byte, val1 []byte) (mergedVal []byte) {
    return []byte(string(val0) + " + " + string(val1))
}

func main() {
    if len(os.Args) < 2 {
        fmt.Fprintf(os.Stderr, "Usage: %s <MTBL FILE>...\n", os.Args[0])
        os.Exit(1)
    }
    fnames := os.Args[1:]

    m := mtbl.MergerInit(&mtbl.MergerOptions{Merge: mergeFunc})
    defer m.Destroy()

    for _, fname := range fnames {
        r, e := mtbl.ReaderInit(fname, nil)
        defer r.Destroy()
        if e != nil {
            fmt.Fprintf(os.Stderr, "Error: %s\n", e)
            os.Exit(1)
        }
        m.AddSource(r)
    }

    it := mtbl.IterAll(m)
    defer it.Destroy()
    for {
        key, val, ok := it.Next()
        if !ok {
            break
        }
        fmt.Printf("%q %q\n", key, val)
    }
}
