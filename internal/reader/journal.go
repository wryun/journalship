// Copyright 2015 RedHat, Inc.
// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Based on https://github.com/coreos/go-systemd/blob/master/sdjournal/journal.go
// and https://github.com/liquidm/elastic-journald/blob/master/service.go
package reader

// #include <stdio.h>
// #include <string.h>
// #include <stdlib.h>
// #include <systemd/sd-journal.h>
// #cgo LDFLAGS: -lsystemd
import "C"

import (
	"fmt"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

const (
	// IndefiniteWait is a sentinel value that can be passed to
	// sdjournal.Wait() to signal an indefinite wait for new journal
	// events. It is implemented as the maximum value for a time.Duration:
	// https://github.com/golang/go/blob/e4dcf5c8c22d98ac9eac7b9b226596229624cb1d/src/time/time.go#L434
	IndefiniteWait time.Duration = 1<<63 - 1

	SD_JOURNAL_NOP        = int(C.SD_JOURNAL_NOP)
	SD_JOURNAL_APPEND     = int(C.SD_JOURNAL_APPEND)
	SD_JOURNAL_INVALIDATE = int(C.SD_JOURNAL_INVALIDATE)
)

type JournalEntry struct {
	Fields map[string]interface{}
}

// NewJournal exists because the coreos library is a little too heavy
// (locking all the time when we don't need it, etc.) and
// doesn't return a map[string]interface{} of fields...
// (we need this for JSON-ish processing)
func NewJournal() (*C.sd_journal, error) {
	var j *C.sd_journal
    r := C.sd_journal_open(&j, C.SD_JOURNAL_LOCAL_ONLY)
	return j, translateError(r)
}

func translateError(r C.int) error {
    if r < 0 {
       return fmt.Errorf("failed to open journal: %d", syscall.Errno(-r)) 
	}
	return nil
}

func (j *C.sd_journal) SeekHead() error {
	return translateError(C.sd_journal_seek_head(j))
}

func (j *C.sd_journal) SeekCursor(cursor string) error {
	c := C.CString(cursor)
	defer C.free(unsafe.Pointer(c))
	return translateError(C.sd_journal_seek_cursor(j, c))
}

func (j *C.sd_journal) Wait(timeout time.Duration) (int, error) {
	var to uint64

	if timeout == IndefiniteWait {
		// sd_journal_wait(3) calls for a (uint64_t) -1 to be passed to signify
		// indefinite wait, but using a -1 overflows our C.uint64_t, so we use an
		// equivalent hex value.
		to = 0xffffffffffffffff
	} else {
		to = uint64(timeout / time.Microsecond)
	}

	r := C.sd_journal_wait(j, C.uint64_t(to))
	return int(r), translateError(r)
}

func (j *C.sd_journal) Next() (uint64, error) {
	r := C.sd_journal_next(j)
	return uint64(r), translateError(r)
}

func (j *C.sd_journal) GetFields() (map[string]interface{}, error) {
	fields := make(map[string]interface{})
	var d unsafe.Pointer
	var l C.size_t
	var r C.int
	C.sd_journal_restart_data(j)
	for {
		r = C.sd_journal_enumerate_data(j, &d, &l)
		if r <= 0 {
			break
		}

		// https://github.com/liquidm/elastic-journald/blob/master/service.go
		// has a more complicated approach (regex match), but I think it's equivalent...
		msg := C.GoStringN((*C.char)(d), C.int(l))
		kv := strings.SplitN(msg, "=", 2)
		if len(kv) < 2 {
			return nil, fmt.Errorf("failed to parse field: %s", msg)
		}

		fields[kv[0]] = kv[1]
	}

	return fields, translateError(r)
}