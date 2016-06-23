package encryptedfile

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/crypto/nacl/secretbox"
)

const pgSize = 4096
const pgCacheMaxSize = 2 << 18 // about 1GB ??
const nonceSize = 24
const dataPgSize = pgSize - secretbox.Overhead - nonceSize

type page struct {
	Data []byte
	pgID int64
}

// EncryptedFile wraps access to an os.File in transparent secretbox encryption
// Satisfies the gkvlite StoreFile interface
type EncryptedFile struct {
	key     [32]byte
	file    *os.File
	pgCache map[int64]page
}

// Open returns an encrypted file
func Open(name string, key [32]byte) (*EncryptedFile, error) {
	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	return &EncryptedFile{key: key, file: file, pgCache: make(map[int64]page)}, nil
}

// Close closes an encrypted file
func (f *EncryptedFile) Close() error {
	return f.file.Close()
}

func (f *EncryptedFile) writePages(pages []page) error {
	encryptedBytes := make([]byte, len(pages)*pgSize)
	for i, pg := range pages {
		nonce := new([nonceSize]byte)
		_, err := io.ReadFull(rand.Reader, nonce[:])
		if err != nil {
			return err
		}
		out := make([]byte, nonceSize)
		copy(out, nonce[:])
		outVal := secretbox.Seal(out, pg.Data, nonce, &f.key)
		copy(encryptedBytes[i*pgSize:(i+1)*pgSize], outVal)
	}
	_, err := f.file.WriteAt(encryptedBytes, pages[0].pgID*pgSize)
	return err
}

func (f *EncryptedFile) loadPages(start int64, end int64) ([]page, error) {
	var pages []page
	startReadOffset := int64(pgSize * start)
	readLen := pgSize * (end + 1 - start)
	bytes := make([]byte, readLen)
	n, err := f.file.ReadAt(bytes, startReadOffset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	for i := int64(0); i < end-start+1; i++ {

		pgID := int64(i + start)
		pg := page{pgID: pgID}
		// if we're off the end of the file, just copy over the empty bytes
		// no decryption necessary
		if (i+1)*pgSize > int64(n) {
			pg.Data = bytes[pgSize*i : pgSize*i+dataPgSize]
			pages = append(pages, pg)
			continue
		}

		dataPg := bytes[pgSize*i : pgSize*(i+1)]
		nonce := new([nonceSize]byte)
		copy(nonce[:], dataPg[:nonceSize])
		data, ok := secretbox.Open(nil, dataPg[nonceSize:], nonce, &f.key)
		if !ok {
			return nil, fmt.Errorf("Decryption failed")
		}
		pg.Data = data
		f.pgCache[pgID] = pg

		pages = append(pages, pg)
	}
	return pages, nil
}

// ReadAt implements ReaderAt
func (f *EncryptedFile) ReadAt(p []byte, off int64) (n int, err error) {
	n = 0
	startPgNum := off / dataPgSize
	startPgOffset := off % dataPgSize
	end := off + int64(len(p))
	endPgNum := end / dataPgSize
	endPgOffset := end % dataPgSize

	pages, err := f.loadPages(startPgNum, endPgNum)
	if err != nil {
		return
	}
	if len(pages) == 1 {
		// start and end page
		n += copy(p[:endPgOffset-startPgOffset], pages[0].Data[startPgOffset:endPgOffset])
	} else {
		for i, pg := range pages {
			if i == 0 {
				// start page
				n += copy(p[:dataPgSize-startPgOffset], pg.Data[startPgOffset:])
			} else if i == len(pages)-1 {
				// end page
				n += copy(p[n:int64(n)+endPgOffset], pg.Data[:endPgOffset])
			} else {
				// middle page
				n += copy(p[n:n+dataPgSize], pg.Data[:])
			}
		}
	}

	return
}

// WriteAt implements WriterAt
func (f *EncryptedFile) WriteAt(p []byte, off int64) (n int, err error) {
	n = 0
	startPgNum := off / dataPgSize
	startPgOffset := off % dataPgSize
	end := off + int64(len(p))
	endPgNum := end / dataPgSize
	endPgOffset := end % dataPgSize

	pages, err := f.loadPages(startPgNum, endPgNum)
	if err != nil {
		return
	}
	if len(pages) == 1 {
		// start and end page
		n += copy(pages[0].Data[startPgOffset:endPgOffset], p[:])
	} else {
		for i, pg := range pages {
			if i == 0 {
				// start page
				n += copy(pg.Data[startPgOffset:], p[:dataPgSize-startPgOffset])
			} else if i == len(pages)-1 {
				// end page
				n += copy(pg.Data[:endPgOffset], p[n:])
			} else {
				// middle page
				n += copy(pg.Data[:], p[n:n+dataPgSize])
			}
		}
	}
	return n, f.writePages(pages)
}

// Truncate implements StoreFile
func (f *EncryptedFile) Truncate(size int64) error {
	r := size % pgSize
	var numPg int64
	if r == 0 {
		numPg = size / pgSize
	} else {
		numPg = (size / pgSize) + 1
	}
	return f.file.Truncate(numPg * pgSize)
}

// FileInfo implements os.FileInfo
type FileInfo struct {
	fi os.FileInfo
}

// Name implements FileInfo
func (e FileInfo) Name() string {
	return e.fi.Name()
}

// Size implements FileInfo
func (e FileInfo) Size() int64 {
	encryptedSize := e.fi.Size()
	numPg := encryptedSize / pgSize
	return dataPgSize * numPg
}

// Mode implements FileInfo
func (e FileInfo) Mode() os.FileMode {
	return e.fi.Mode()
}

// ModTime implements FileInfo
func (e FileInfo) ModTime() time.Time {
	return e.fi.ModTime()
}

// IsDir implements FileInfo
func (e FileInfo) IsDir() bool {
	return e.fi.IsDir()

}

// Sys implements FileInfo
func (e FileInfo) Sys() interface{} {
	return nil
}

// Stat implements StoreFile
func (f *EncryptedFile) Stat() (os.FileInfo, error) {
	fileInfo, err := f.file.Stat()
	return FileInfo{fi: fileInfo}, err
}
