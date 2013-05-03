package mtbl

/*
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
*/
import "C"

import "reflect"
import "unsafe"

//export go_merge_callback
func go_merge_callback(pfunc unsafe.Pointer,
                       pkey *C.uint8_t, len_key C.size_t,
                       pval0 *C.uint8_t, len_val0 C.size_t,
                       pval1 *C.uint8_t, len_val1 C.size_t,
                       pmerged_val **C.uint8_t, len_merged_val *C.size_t) {
    if merge := (*MergeFunc)(pfunc); merge != nil {
        var key []byte
        var val0 []byte
        var val1 []byte

        sh_key := (*reflect.SliceHeader)(unsafe.Pointer(&key))
        sh_key.Data = uintptr(unsafe.Pointer(pkey))
        sh_key.Len = int(len_key)
        sh_key.Cap = int(len_key)

        sh_val0 := (*reflect.SliceHeader)(unsafe.Pointer(&val0))
        sh_val0.Data = uintptr(unsafe.Pointer(pval0))
        sh_val0.Len = int(len_val0)
        sh_val0.Cap = int(len_val0)

        sh_val1 := (*reflect.SliceHeader)(unsafe.Pointer(&val1))
        sh_val1.Data = uintptr(unsafe.Pointer(pval1))
        sh_val1.Len = int(len_val1)
        sh_val1.Cap = int(len_val1)

        merged_val := (*merge)(key, val0, val1)

        *pmerged_val = (*C.uint8_t)(C.malloc(C.size_t(len(merged_val))))
        *len_merged_val = (C.size_t)(len(merged_val))

        C.memcpy(
            unsafe.Pointer(*pmerged_val),
            unsafe.Pointer(&merged_val[0]),
            C.size_t(len(merged_val)),
        )
    } else {
        panic("go_merge_callback() got a nil callback function");
    }
}
