package mtbl

/*
#include <stdint.h>
#include <stdlib.h>

#cgo pkg-config: libmtbl
#include <mtbl.h>

void go_merge_callback(void *,
    const uint8_t *, size_t,
    const uint8_t *, size_t,
    const uint8_t *, size_t,
    uint8_t **, size_t *);

static void
set_merger_callback_go(struct mtbl_merger_options *mopt, void *clos)
{
    mtbl_merger_options_set_merge_func(mopt, go_merge_callback, clos);
}

static void
set_merger_callback_c(struct mtbl_merger_options *mopt, void *fp, void *clos)
{
    mtbl_merger_options_set_merge_func(mopt, fp, clos);
}

static void
set_sorter_callback_go(struct mtbl_sorter_options *sopt, void *clos)
{
    mtbl_sorter_options_set_merge_func(sopt, go_merge_callback, clos);
}

static void
set_sorter_callback_c(struct mtbl_sorter_options *sopt, void *fp, void *clos)
{
    mtbl_sorter_options_set_merge_func(sopt, fp, clos);
}

static void
set_fileset_callback_go(struct mtbl_fileset_options *fopt, void *clos)
{
    mtbl_fileset_options_set_merge_func(fopt, go_merge_callback, clos);
}

static void
set_fileset_callback_c(struct mtbl_fileset_options *fopt, void *fp, void *clos)
{
    mtbl_fileset_options_set_merge_func(fopt, fp, clos);
}

*/
import "C"

import "container/list"
import "fmt"
import "reflect"
import "runtime"
import "unsafe"

/* constants */

const COMPRESSION_NONE = C.MTBL_COMPRESSION_NONE
const COMPRESSION_SNAPPY = C.MTBL_COMPRESSION_SNAPPY
const COMPRESSION_ZLIB = C.MTBL_COMPRESSION_ZLIB
const COMPRESSION_LZ4 = C.MTBL_COMPRESSION_LZ4
const COMPRESSION_LZ4HC = C.MTBL_COMPRESSION_LZ4HC

/* Iter */

type Iter struct {
	cptr *C.struct_mtbl_iter
	s    Source
}

func iterInit(s Source, c_it *C.struct_mtbl_iter) (it *Iter) {
	it = new(Iter)
	it.cptr = c_it
	it.s = s
	runtime.SetFinalizer(it, func(it *Iter) { it.Destroy() })
	return
}

func (it *Iter) Destroy() {
	if it.cptr != nil {
		C.mtbl_iter_destroy(&it.cptr)
	}
	it = nil
}

func (it Iter) Next() (key []byte, val []byte, ok bool) {
	var c_len_key C.size_t
	var c_len_val C.size_t
	var c_key *C.uint8_t
	var c_val *C.uint8_t

	res := C.mtbl_iter_next(it.cptr, &c_key, &c_len_key, &c_val, &c_len_val)
	if res == C.mtbl_res_success {
		k := (*reflect.SliceHeader)(unsafe.Pointer(&key))
		k.Data = uintptr(unsafe.Pointer(c_key))
		k.Len = int(c_len_key)
		k.Cap = int(c_len_key)

		v := (*reflect.SliceHeader)(unsafe.Pointer(&val))
		v.Data = uintptr(unsafe.Pointer(c_val))
		v.Len = int(c_len_val)
		v.Cap = int(c_len_val)

		ok = true
	} else {
		ok = false
	}
	return
}

func (it *Iter) String() string {
	return fmt.Sprintf("<mtbl.Iter at %p on %s>", it, it.s)
}

/* Source */

type Source interface {
	getSource() *C.struct_mtbl_source
}

func Get(s Source, key []byte) (val []byte, ok bool) {
	it := iterInit(s, C.mtbl_source_get(s.getSource(),
		(*C.uint8_t)(&key[0]), C.size_t(len(key))))
	defer it.Destroy()
	_, val, ok = it.Next()
	return
}

func IterAll(s Source) (it *Iter) {
	return iterInit(s, C.mtbl_source_iter(s.getSource()))
}

func IterPrefix(s Source, prefix []byte) (it *Iter) {
	return iterInit(s, C.mtbl_source_get_prefix(s.getSource(),
		(*C.uint8_t)(&prefix[0]), C.size_t(len(prefix))))
}

func IterRange(s Source, key0 []byte, key1 []byte) (it *Iter) {
	return iterInit(s, C.mtbl_source_get_range(s.getSource(),
		(*C.uint8_t)(&key0[0]), C.size_t(len(key0)),
		(*C.uint8_t)(&key1[0]), C.size_t(len(key1))))
}

func Write(s Source, w Writer) (e error) {
	if w.cptr == nil {
		panic("w.cptr is nil")
	}
	if C.mtbl_source_write(s.getSource(), w.cptr) != C.mtbl_res_success {
		e = fmt.Errorf("mtbl_source_write() failed")
	}
	return
}

/* Reader */

type Reader struct {
	cptr *C.struct_mtbl_reader
}

type ReaderOptions struct {
	VerifyChecksums bool
}

func ReaderInit(fname string, ropt *ReaderOptions) (r *Reader, e error) {
	var c_ropt *C.struct_mtbl_reader_options
	if ropt != nil {
		c_ropt = C.mtbl_reader_options_init()
		defer C.mtbl_reader_options_destroy(&c_ropt)
		C.mtbl_reader_options_set_verify_checksums(c_ropt, C.bool(ropt.VerifyChecksums))
	}

	c_fname := C.CString(fname)
	defer C.free(unsafe.Pointer(c_fname))

	r = new(Reader)
	r.cptr = C.mtbl_reader_init(c_fname, c_ropt)
	if r.cptr == nil {
		e = fmt.Errorf("mtbl_reader_init(%q) failed", fname)
	}
	runtime.SetFinalizer(r, func(r *Reader) { r.Destroy() })
	return
}

func (r *Reader) Destroy() {
	C.mtbl_reader_destroy(&r.cptr)
	r = nil
}

func (r Reader) getSource() *C.struct_mtbl_source {
	if r.cptr == nil {
		panic("r.cptr is nil")
	}
	return C.mtbl_reader_source(r.cptr)
}

func (r *Reader) String() string {
	return fmt.Sprintf("<mtbl.Reader at %p>", r)
}

/* Writer */

type Writer struct {
	cptr *C.struct_mtbl_writer
}

type WriterOptions struct {
	Compression          int
	BlockSize            int
	BlockRestartInterval int
}

func WriterInit(fname string, wopt *WriterOptions) (w *Writer, e error) {
	var c_wopt *C.struct_mtbl_writer_options
	if wopt != nil {
		c_wopt = C.mtbl_writer_options_init()
		defer C.mtbl_writer_options_destroy(&c_wopt)
		C.mtbl_writer_options_set_compression(c_wopt, C.mtbl_compression_type(wopt.Compression))
		if wopt.BlockSize != 0 {
			C.mtbl_writer_options_set_block_size(c_wopt, C.size_t(wopt.BlockSize))
		}
		if wopt.BlockRestartInterval != 0 {
			C.mtbl_writer_options_set_block_restart_interval(c_wopt,
				C.size_t(wopt.BlockRestartInterval))
		}
	}

	c_fname := C.CString(fname)
	defer C.free(unsafe.Pointer(c_fname))

	w = new(Writer)
	w.cptr = C.mtbl_writer_init(c_fname, c_wopt)
	if w.cptr == nil {
		e = fmt.Errorf("mtbl_writer_init(%q) failed", fname)
	}
	runtime.SetFinalizer(w, func(w *Writer) { w.Destroy() })
	return
}

func (w *Writer) Destroy() {
	C.mtbl_writer_destroy(&w.cptr)
	w = nil
}

func (w *Writer) Add(key []byte, val []byte) (e error) {
	res := C.mtbl_writer_add(w.cptr,
		(*C.uint8_t)(&key[0]), C.size_t(len(key)),
		(*C.uint8_t)(&val[0]), C.size_t(len(val)))
	if res != C.mtbl_res_success {
		e = fmt.Errorf("mtbl_writer_add(%q, %q) failed", key, val)
	}
	return
}

func (w *Writer) String() string {
	return fmt.Sprintf("<mtbl.Writer at %p>", w)
}

/* Merger */

type MergeFunc func(key []byte, val0 []byte, val1 []byte) (mergedVal []byte)

type MergerOptions struct {
	Merge      MergeFunc
	CMerge     unsafe.Pointer
	CMergeData unsafe.Pointer
}

type Merger struct {
	cptr       *C.struct_mtbl_merger
	merge      MergeFunc
	sourceList *list.List
}

func MergerInit(mopt *MergerOptions) (m *Merger) {
	if mopt == nil {
		panic("mopt is nil")
	}
	if mopt.Merge == nil && mopt.CMerge == nil {
		panic("need a merging function")
	}
	if mopt.Merge != nil && mopt.CMerge != nil {
		panic("need exactly one of Merge or CMerge")
	}

	m = new(Merger)
	m.sourceList = list.New()

	c_mopt := C.mtbl_merger_options_init()
	defer C.mtbl_merger_options_destroy(&c_mopt)
	if mopt.Merge != nil {
		m.merge = mopt.Merge // keep gc reference
		C.set_merger_callback_go(c_mopt, unsafe.Pointer(&m.merge))
	} else if mopt.CMerge != nil {
		C.set_merger_callback_c(c_mopt, mopt.CMerge, mopt.CMergeData)
	}

	m.cptr = C.mtbl_merger_init(c_mopt)
	runtime.SetFinalizer(m, func(m *Merger) { m.Destroy() })
	return
}

func (m *Merger) AddSource(s Source) {
	if m.cptr == nil {
		panic("m.cptr is nil")
	}
	m.sourceList.PushBack(&s)
	C.mtbl_merger_add_source(m.cptr, s.getSource())
}

func (m *Merger) Destroy() {
	if m.cptr == nil {
		panic("m.cptr is nil")
	}
	C.mtbl_merger_destroy(&m.cptr)
	m = nil
}

func (m *Merger) getSource() *C.struct_mtbl_source {
	if m.cptr == nil {
		panic("m.cptr is nil")
	}
	return C.mtbl_merger_source(m.cptr)
}

func (m *Merger) String() string {
	return fmt.Sprintf("<mtbl.Merger at %p on %d sources>", m, m.sourceList.Len())
}

/* Sorter */

type SorterOptions struct {
	TempDir    string
	MaxMemory  uint64
	Merge      MergeFunc
	CMerge     unsafe.Pointer
	CMergeData unsafe.Pointer
}

type Sorter struct {
	cptr  *C.struct_mtbl_sorter
	merge MergeFunc
}

func SorterInit(sopt *SorterOptions) (s *Sorter) {
	if sopt == nil {
		panic("sopt is nil")
	}
	if sopt.Merge == nil && sopt.CMerge == nil {
		panic("need a merging function")
	}
	if sopt.Merge != nil && sopt.CMerge != nil {
		panic("need exactly one of Merge or CMerge")
	}

	s = new(Sorter)

	c_sopt := C.mtbl_sorter_options_init()
	defer C.mtbl_sorter_options_destroy(&c_sopt)
	if sopt.TempDir != "" {
		c_temp_dir := C.CString(sopt.TempDir)
		defer C.free(unsafe.Pointer(c_temp_dir))
		C.mtbl_sorter_options_set_temp_dir(c_sopt, c_temp_dir)
	}
	if sopt.MaxMemory != 0 {
		C.mtbl_sorter_options_set_max_memory(c_sopt, C.size_t(sopt.MaxMemory))
	}
	if sopt.Merge != nil {
		s.merge = sopt.Merge // keep gc reference
		C.set_sorter_callback_go(c_sopt, unsafe.Pointer(&s.merge))
	} else if sopt.CMerge != nil {
		C.set_sorter_callback_c(c_sopt, sopt.CMerge, sopt.CMergeData)
	}

	s.cptr = C.mtbl_sorter_init(c_sopt)
	runtime.SetFinalizer(s, func(s *Sorter) { s.Destroy() })
	return
}

func (s *Sorter) Destroy() {
	if s.cptr == nil {
		panic("s.cptr is nil")
	}
	C.mtbl_sorter_destroy(&s.cptr)
	s = nil
}

func (s *Sorter) Add(key []byte, val []byte) (e error) {
	res := C.mtbl_sorter_add(s.cptr,
		(*C.uint8_t)(&key[0]), C.size_t(len(key)),
		(*C.uint8_t)(&val[0]), C.size_t(len(val)))
	if res != C.mtbl_res_success {
		e = fmt.Errorf("mtbl_sorter_add(%q, %q) failed", key, val)
	}
	return
}

func (s *Sorter) Write(w *Writer) (e error) {
	res := C.mtbl_sorter_write(s.cptr, w.cptr)
	if res != C.mtbl_res_success {
		e = fmt.Errorf("mtbl_sorter_write failed")
	}
	return
}

func (s *Sorter) String() string {
	return fmt.Sprintf("<mtbl.Sorter at %p>", s)
}

/* Fileset */

type FilesetOptions struct {
	ReloadInterval uint32
	Merge          MergeFunc
	CMerge         unsafe.Pointer
	CMergeData     unsafe.Pointer
}

type Fileset struct {
	cptr  *C.struct_mtbl_fileset
	merge MergeFunc
	fname string
}

func FilesetInit(fname string, fopt *FilesetOptions) (f *Fileset, e error) {
	if fopt == nil {
		panic("fopt is nil")
	}
	if fopt.Merge == nil && fopt.CMerge == nil {
		panic("need a merging function")
	}
	if fopt.Merge != nil && fopt.CMerge != nil {
		panic("need exactly one of Merge or CMerge")
	}

	f = new(Fileset)
	f.fname = fname

	c_fopt := C.mtbl_fileset_options_init()
	defer C.mtbl_fileset_options_destroy(&c_fopt)
	if fopt.ReloadInterval != 0 {
		C.mtbl_fileset_options_set_reload_interval(c_fopt, C.uint32_t(fopt.ReloadInterval))
	}
	if fopt.Merge != nil {
		f.merge = fopt.Merge // keep gc reference
		C.set_fileset_callback_go(c_fopt, unsafe.Pointer(&f.merge))
	} else if fopt.CMerge != nil {
		C.set_fileset_callback_c(c_fopt, fopt.CMerge, fopt.CMergeData)
	}

	c_fname := C.CString(fname)
	defer C.free(unsafe.Pointer(c_fname))

	f.cptr = C.mtbl_fileset_init(c_fname, c_fopt)
	if f.cptr == nil {
		e = fmt.Errorf("mtbl_fileset_init(%q) failed", fname)
	}
	runtime.SetFinalizer(f, func(f *Fileset) { f.Destroy() })
	return
}

func (f *Fileset) Destroy() {
	if f.cptr == nil {
		panic("f.cptr is nil")
	}
	C.mtbl_fileset_destroy(&f.cptr)
	f = nil
}

func (f *Fileset) getSource() *C.struct_mtbl_source {
	if f.cptr == nil {
		panic("f.cptr is nil")
	}
	return C.mtbl_fileset_source(f.cptr)
}

func (f *Fileset) String() string {
	return fmt.Sprintf("<mtbl.Fileset at %p from setfile %s>", f, f.fname)
}
