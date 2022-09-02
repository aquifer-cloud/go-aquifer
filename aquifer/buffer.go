package aquifer

import (
	"io"
	"bytes"
	"sync"
)

type ThreadsafeBuffer struct {
	buffer bytes.Buffer
	mutex sync.Mutex
}

func (buf *ThreadsafeBuffer) Len() int {
	buf.mutex.Lock()
	defer buf.mutex.Unlock()
	return buf.buffer.Len()
}

func (buf *ThreadsafeBuffer) Read(p []byte) (n int, err error) {
	buf.mutex.Lock()
	defer buf.mutex.Unlock()
	return buf.buffer.Read(p)
}

func (buf *ThreadsafeBuffer) Write(p []byte) (n int, err error) {
	buf.mutex.Lock()
	defer buf.mutex.Unlock()
	return buf.buffer.Write(p)
}

func (buf *ThreadsafeBuffer) WriteTo(w io.Writer) (n int64, err error) {
	buf.mutex.Lock()
	defer buf.mutex.Unlock()
	return buf.buffer.WriteTo(w)
}

func (buf *ThreadsafeBuffer) Next(n int) []byte {
	buf.mutex.Lock()
	defer buf.mutex.Unlock()
	return buf.buffer.Next(n)
}