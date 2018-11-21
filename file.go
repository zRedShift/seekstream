// Package seekstream provides a way to treat streaming data as if it were a regular file, blocking until more data is
// available or the end is reached.
package seekstream

import (
	"io"
	"io/ioutil"
	"os"
	"sync/atomic"
)

// bufSize is the default buffer size of io.copyBuffer.
const bufSize = 1 << 15

// File is the seekable streaming data in the form of a file. Initialised using NewFile.
type File struct {
	file       *os.File
	r, w       int64
	done, wait chan struct{}
}

// NewFile creates a new temporary file in the (optionally provided) directory and returns an empty File.
func NewFile(tempDir ...string) (*File, error) {
	TempDir := os.TempDir()
	if len(tempDir) > 0 {
		TempDir = tempDir[0]
	}

	file, err := ioutil.TempFile(TempDir, "stream_")
	if err != nil {
		return nil, err
	}

	return &File{
		file: file,
		done: make(chan struct{}),
		wait: make(chan struct{}),
	}, nil
}

func (f *File) notify() {
	for {
		select {
		case <-f.wait:

		default:
			return
		}
	}
}

// Write implements the io.Writer interface and notifies all the blocked readers after each write.
func (f *File) Write(p []byte) (int, error) {
	if f.IsDone() {
		return 0, &os.PathError{Op: "write", Path: f.Name(), Err: os.ErrClosed}
	}

	n, err := f.file.WriteAt(p, f.w)
	atomic.AddInt64(&f.w, int64(n))

	f.notify()

	return n, err
}

// Read implements the io.Reader interface and only blocks in the case of a (0, nil) Read.
func (f *File) Read(p []byte) (int, error) {
	if !f.block(f.r) {
		return 0, io.EOF
	}

	p = subSlice(p, int(atomic.LoadInt64(&f.w)-f.r))

	n, err := f.file.Read(p)
	f.r += int64(n)
	return n, err
}

// ReadAt implements the ReaderAt interface, blocking until the buffer is filled or the EOF reached.
func (f *File) ReadAt(p []byte, off int64) (int, error) {
	if !f.block(off) {
		return 0, io.EOF
	}

	var (
		err  error
		pLen = len(p)
	)

	for {
		n := 0
		n, err = f.file.ReadAt(subSlice(p, int(atomic.LoadInt64(&f.w)-off)), off)
		p = p[n:]
		off += int64(n)
		if err != nil || len(p) == 0 {
			break
		}

		if !f.block(off) {
			err = io.EOF
			break
		}
	}

	return pLen - len(p), err
}

func (f *File) block(off int64) bool {
	for off >= atomic.LoadInt64(&f.w) {
		select {
		case <-f.done:
			return off < f.w

		case f.wait <- struct{}{}:
		}
	}

	return true
}

// Seek implements the io.Seeker interface, blocking until file size is known if the whence is io.SeekEnd.
func (f *File) Seek(off int64, whence int) (int64, error) {
	var err error

	if whence == io.SeekEnd {
		f.Wait()
	}

	f.r, err = f.file.Seek(off, whence)
	return f.r, err
}

// DoneWriting closes the File for writing.
func (f *File) DoneWriting() {
	select {
	case <-f.done:

	default:
		close(f.done)
	}
}

// ReadFrom implements the ReaderFrom interface.
func (f *File) ReadFrom(r io.Reader) (int64, error) {
	var (
		err      error
		startPos = atomic.LoadInt64(&f.w)
	)

	buf := make([]byte, bufSize)

	for {
		nr, er := r.Read(buf)
		if nr > 0 {
			nw, ew := f.Write(buf[:nr])
			if ew != nil {
				err = ew
				break
			}

			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}

		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}

	return atomic.LoadInt64(&f.w) - startPos, err
}

// Wait blocks until the File is closed for writing.
func (f *File) Wait() {
	<-f.done
}

// IsDone returns whether DoneWriting was called on the File.
func (f *File) IsDone() bool {
	select {
	case <-f.done:
		return true

	default:
		return false
	}
}

// Name returns the temporary file's path.
func (f *File) Name() string {
	return f.file.Name()
}

// Size wait for the File to be closed for writing, and returns the final file size.
func (f *File) Size() int64 {
	f.Wait()
	return f.w
}

// Close closes the temporary file.
func (f *File) Close() error {
	f.DoneWriting()

	return f.file.Close()
}

// Move moves the temporary file to the new path.
func (f *File) Move(path string) error {
	if err := f.Close(); err != nil {
		if pErr, ok := err.(*os.PathError); !ok || pErr.Err != os.ErrClosed {
			return err
		}
	}

	return os.Rename(f.file.Name(), path)
}

// Remove removes the temporary file.
func (f *File) Remove() error {
	if err := f.Close(); err != nil {
		if pErr, ok := err.(*os.PathError); !ok || pErr.Err != os.ErrClosed {
			return err
		}
	}

	return os.Remove(f.file.Name())
}
