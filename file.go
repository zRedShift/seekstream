// Package seekstream provides a way to treat streaming data as if it were a regular file, blocking until more data is
// available or the end is reached.
package seekstream

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
)

// bufSize is the default buffer size of io.copyBuffer.
const bufSize = 1 << 15

var (
	errDoneWriting   = errors.New("already done writing")
	errAlreadyClosed = errors.New("file already closed")
	errInvalidOffset = errors.New("invalid offset")
	errInvalidWhence = errors.New("seek: invalid whence")
)

// File is the seekable streaming data in the form of a file. Initialised using NewFile.
type File struct {
	file         *os.File
	r, w         int64
	done, closed bool
	ready        chan struct{}
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
		file:  file,
		ready: make(chan struct{}),
	}, nil
}

// Write implements the io.Writer interface and unblocks the File for reading.
func (f *File) Write(p []byte) (int, error) {
	if f.done {
		return 0, errDoneWriting
	}

	n, err := f.file.WriteAt(p, f.w)
	f.w += int64(n)

	select {
	case f.ready <- struct{}{}:

	default:

	}

	return n, err
}

// Read implements the io.Reader interface and only blocks in the case of a (0, nil) Read.
func (f *File) Read(p []byte) (int, error) {
	err := f.readCommon(f.r)
	if err != nil {
		return 0, err
	}

	p = subSlice(p, int(f.w-f.r))

	n, err := f.file.Read(p)
	f.r += int64(n)
	return n, err
}

// ReadAt implements the ReaderAt interface, blocking until the buffer is filled or the EOF reached.
func (f *File) ReadAt(p []byte, off int64) (int, error) {
	err := f.readCommon(off)
	if err != nil {
		return 0, err
	}

	pLen := len(p)
	for {
		var n int
		n, err = f.file.ReadAt(subSlice(p, int(f.w-off)), off)
		p = p[n:]
		off += int64(n)
		if err != nil || len(p) == 0 {
			break
		}

		if err = f.block(off); err != nil {
			break
		}
	}

	return pLen - len(p), err
}

func (f *File) readCommon(off int64) error {
	switch {
	case f.closed:
		return errAlreadyClosed

	case off < 0:
		return errInvalidOffset

	}

	return f.block(off)
}

func (f *File) block(off int64) error {
	for off >= f.w {
		if f.done {
			return io.EOF
		}
		<-f.ready
	}

	return nil
}

// Seek implements the io.Seeker interface, blocking when the requested offset/whence combination hasn't been written
// yet, or is unknowable.
func (f *File) Seek(off int64, whence int) (int64, error) {
	var newPos int64
	var err error

	switch whence {
	default:
		return 0, errInvalidWhence

	case io.SeekStart:
		newPos = off

	case io.SeekCurrent:
		newPos = f.r + off

	case io.SeekEnd:
		newPos = 1<<63 - 1

	}

	for !f.done && f.w < newPos {
		<-f.ready
	}

	f.r, err = f.file.Seek(off, whence)
	return f.r, err
}

// DoneWriting closes the File for writing, closing the notification channel, automatically clearing all blocks.
func (f *File) DoneWriting() {
	if !f.done {
		f.done = true
		close(f.ready)
	}
}

// ReadFrom implements the ReaderFrom interface. Uses the input's WriterTo interface if available.
func (f *File) ReadFrom(r io.Reader) (int64, error) {
	var err error

	if wt, ok := r.(io.WriterTo); ok {
		return wt.WriteTo(f)
	}

	startPos := f.w
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

	return f.w - startPos, err
}

// Wait blocks until the File is closed for writing.
func (f *File) Wait() {
	for !f.done {
		<-f.ready
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
	if f.closed {
		return nil
	}

	f.DoneWriting()

	err := f.file.Close()
	if err != nil {
		return err
	}

	f.closed = true
	return nil
}

// Move moves the temporary file to the new path.
func (f *File) Move(path string) error {
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(f.file.Name(), path)
}

// Remove removes the temporary file.
func (f *File) Remove() error {
	if err := f.Close(); err != nil {
		return err
	}

	return os.Remove(f.file.Name())
}
