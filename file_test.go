package seekstream

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"testing/iotest"
)

func TestNewFile(t *testing.T) {
	tests := []struct {
		name    string
		tempDir []string
		wantErr bool
	}{
		{"os temp dir", nil, false},
		{"current dir", []string{"."}, false},
		{"non-existent dir", []string{"nonexistent/"}, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, err := NewFile(test.tempDir...)
			if (err != nil) != test.wantErr {
				t.Fatalf("NewFile() error = %v, wantErr %v", err, test.wantErr)
			}

			if test.wantErr {
				return
			}

			wantErr := &os.PathError{Op: "readat", Path: f.Name(), Err: errors.New("negative offset")}
			if _, err = f.ReadAt([]byte{0}, -1); !reflect.DeepEqual(err, wantErr) {
				t.Errorf("File.ReadAt() error = %v, want %v", err, wantErr)
			}

			f.DoneWriting()
			if done := f.IsDone(); !done {
				t.Errorf("File.IsDone() = %v, want %v", done, true)
			}

			if f.Size() != 0 {
				t.Errorf("File.Size() = %v, want %v", f.Size(), 0)
			}

			if _, err = f.ReadAt([]byte{0}, f.w); err != io.EOF {
				t.Errorf("File.ReadAt() error = %v, want %v", err, io.EOF)
			}

			wantErr = &os.PathError{Op: "write", Path: f.Name(), Err: os.ErrClosed}
			if _, err = f.Write([]byte{0}); !reflect.DeepEqual(err, wantErr) {
				t.Errorf("File.Write() error = %v, want %v", err, wantErr)
			}

			if err = f.Remove(); err != nil {
				t.Errorf("File.Remove() error = %v", err)
			}
		})
	}
}

func TestFile_ReadFrom(t *testing.T) {
	tests := []struct {
		name    string
		reader  io.Reader
		want    int64
		wantErr bool
	}{
		{"short", io.LimitReader(rand.Reader, 1<<6), 1 << 6, false},
		{"long", io.LimitReader(rand.Reader, 1<<20), 1 << 20, false},
		{"dataErrReader", iotest.DataErrReader(io.LimitReader(rand.Reader, 1<<10)), 1 << 10, false},
		{"timeoutReader", iotest.TimeoutReader(io.LimitReader(rand.Reader, 1<<10)), 1 << 10, true},
		{"halfReader", iotest.HalfReader(io.LimitReader(rand.Reader, 1<<10)), 1 << 10, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, err := NewFile()
			if err != nil {
				t.Fatalf("NewFile() error = %v", err)
			}

			defer func() {
				if err = f.Remove(); err != nil {
					t.Errorf("File.Remove() error = %v", err)
				}
			}()

			gotErr := make(chan struct {
				int64
				error
			})
			go func() {
				got, er := f.ReadFrom(test.reader)
				f.DoneWriting()
				gotErr <- struct {
					int64
					error
				}{got, er}
				close(gotErr)
			}()

			buf := new(bytes.Buffer)
			if _, err = io.Copy(buf, f); err != nil {
				t.Errorf("io.Copy() err = %v", err)
			}

			got := <-gotErr

			if (got.error != nil) != test.wantErr {
				t.Fatalf("File.ReadFrom() error = %v, wantErr %v", got.error, test.wantErr)
			}

			if got.int64 != test.want {
				t.Errorf("File.ReadFrom() = %v, want %v", got.int64, test.want)
			}
		})
	}
}

func prepareFiles(t *testing.T) (*os.File, *File) {
	t.Helper()
	const bufSize = 1 << 20

	tempFile, err := ioutil.TempFile(os.TempDir(), "tmp_")
	if err != nil {
		t.Fatalf("ioutil.TempFile() error = %v", err)
	}

	defer func() {
		if t.Failed() {
			if err = tempFile.Close(); err != nil {
				t.Logf("os.File.Close() error = %v", err)
			}
			if err = os.Remove(tempFile.Name()); err != nil {
				t.Logf("os.Remove() error = %v", err)
			}
		}
	}()

	n, err := io.CopyN(tempFile, rand.Reader, bufSize)
	if err != nil || n != bufSize {
		t.Fatalf("io.CopyN() = (%v, %v), want (%v, %v)", n, err, bufSize, error(nil))
	}

	if _, err = tempFile.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("os.File.Seek() error = %v", err)
	}

	f, err := NewFile()
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}

	go func() {
		if _, err := f.ReadFrom(tempFile); err != nil {
			t.Errorf("File.ReadFrom() error = %v", err)
		}
		f.DoneWriting()
	}()

	return tempFile, f
}

func TestFile_ReadAt(t *testing.T) {
	const bufSize = 1 << 20
	buf1, buf2, buf3 := make([]byte, bufSize), make([]byte, bufSize), make([]byte, bufSize*2)

	tempFile, f := prepareFiles(t)
	defer func() {
		err := tempFile.Close()
		if err != nil {
			t.Errorf("os.File.Close() error = %v", err)
		}
		if err = os.Remove(tempFile.Name()); err != nil {
			t.Errorf("os.Remove() error = %v", err)
		}
		if err = f.Remove(); err != nil {
			t.Errorf("File.Remove() error = %v", err)
		}
	}()

	nf, err := f.ReadAt(buf1, 0)
	if err != nil || nf != bufSize {
		t.Errorf("File.ReadAt() = (%v, %v), want (%v, %v)", nf, err, bufSize, error(nil))
	}

	nb, err := f.ReadAt(buf3, 0)
	if err != io.EOF || nb != bufSize {
		t.Errorf("File.ReadAt() = (%v, %v), want (%v, %v)", nb, err, bufSize, io.EOF)
	}

	nt, err := tempFile.ReadAt(buf2, 0)
	if err != nil || nt != bufSize {
		t.Errorf("os.File.ReadAt() = (%v, %v), want (%v, %v)", nt, err, bufSize, error(nil))
	}

	if !bytes.Equal(buf1, buf2) {
		t.Errorf("File.ReadAt() != os.File.ReadAt()")
	}

	f.Wait()
}

func TestFile_Seek(t *testing.T) {
	tests := []struct {
		name    string
		off     int64
		whence  int
		want    int64
		wantErr bool
	}{
		{"start", 1 << 20, io.SeekStart, 1 << 20, false},
		{"current", 1 << 20, io.SeekCurrent, 1 << 20, false},
		{"end", -1 << 20, io.SeekEnd, 0, false},
		{"invalid whence", 0, -1, 0, true},
		{"invalid offset", -1, io.SeekStart, 0, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempFile, f := prepareFiles(t)
			defer func() {
				err := tempFile.Close()
				if err != nil {
					t.Errorf("os.File.Close() error = %v", err)
				}
				if err = os.Remove(tempFile.Name()); err != nil {
					t.Errorf("os.Remove() error = %v", err)
				}
				if err = f.Remove(); err != nil {
					t.Errorf("File.Remove() error = %v", err)
				}
			}()

			n, err := f.Seek(test.off, test.whence)
			if (err != nil) != test.wantErr {
				t.Fatalf("File.Seek() error = %v, wantErr %v", err, test.wantErr)
			}

			if n != test.want {
				t.Errorf("File.Seek() = %v, want %v", n, test.want)
			}

			f.Wait()
		})
	}

}

func TestFile_Move(t *testing.T) {
	tempFile, f := prepareFiles(t)

	f.Wait()

	err := tempFile.Close()
	if err != nil {
		t.Errorf("os.File.Close() error = %v", err)
	}
	if err = os.Remove(tempFile.Name()); err != nil {
		t.Errorf("os.Remove() error = %v", err)
	}

	if err = f.Move(f.Name() + "_"); err != nil {
		t.Errorf("File.Move() error = %v", err)
	}

	if err = f.Move(f.Name()); err == nil {
		t.Errorf("File.Move() error = %v, wantErr %v", err, true)
	}

	if err = f.Remove(); err == nil {
		t.Errorf("File.Remove() error = %v, wantErr %v", err, true)
	}

	if err = os.Rename(f.file.Name()+"_", f.file.Name()); err != nil {
		t.Errorf("os.Rename() error = %v", err)
	}

	if err = f.Remove(); err != nil {
		t.Errorf("File.Remove() error = %v", err)
	}

	wantErr := &os.PathError{Op: "read", Path: f.Name(), Err: os.ErrClosed}

	if _, err = f.Read([]byte{}); !reflect.DeepEqual(err, wantErr) {
		t.Errorf("File.Read() error = %v, want %v", err, wantErr)
	}

	f.done = make(chan struct{})
	if err = f.Remove(); err == nil {
		t.Errorf("File.Remove() error = %v, wantErr %v", err, true)
	}

	f.done = make(chan struct{})
	if err = f.Move(f.Name()); err == nil {
		t.Errorf("File.Move() error = %v, wantErr %v", err, true)
	}
}

func TestFile_nil(t *testing.T) {
	var (
		f = &File{
			done: make(chan struct{}),
			wait: make(chan struct{}),
		}
		n   int64
		err error
	)

	if n, err = f.ReadFrom(rand.Reader); err != os.ErrInvalid || n != 0 {
		t.Errorf("File.ReadFrom() = (%v, %v), want (%v, %v)", n, err, 0, os.ErrInvalid)
	}

	if err = f.Move(""); err != os.ErrInvalid {
		t.Errorf("File.Move() error = %v, wantErr %v", err, os.ErrInvalid)
	}

	if err = f.Remove(); err != os.ErrInvalid {
		t.Errorf("File.Remove() error = %v, wantErr %v", err, os.ErrInvalid)
	}
}
