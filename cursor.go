package h2a

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Cursor keeps the seq of the last reply the service handled, across
// restarts: the next hello asks for what came after it (resume_after).
type Cursor interface {
	Load() (int64, error)
	Save(seq int64) error
}

// MemCursor is a Cursor in memory: a restart starts from every reply not yet
// acknowledged.
func MemCursor() Cursor { return &memCursor{} }

type memCursor struct {
	mu  sync.Mutex
	seq int64
}

func (m *memCursor) Load() (int64, error) { m.mu.Lock(); defer m.mu.Unlock(); return m.seq, nil }
func (m *memCursor) Save(seq int64) error { m.mu.Lock(); defer m.mu.Unlock(); m.seq = seq; return nil }

// FileCursor is a Cursor in a file: the seq as text, written atomically (a
// temp file renamed over it). A missing file is 0.
func FileCursor(path string) Cursor { return fileCursor{path} }

type fileCursor struct{ path string }

func (f fileCursor) Load() (int64, error) {
	b, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
}

func (f fileCursor) Save(seq int64) error {
	tmp, err := os.CreateTemp(filepath.Dir(f.path), filepath.Base(f.path)+".*")
	if err != nil {
		return err
	}
	if _, err := tmp.WriteString(strconv.FormatInt(seq, 10) + "\n"); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), f.path)
}
