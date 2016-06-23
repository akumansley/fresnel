package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"log"
  "bytes"

	"github.com/awans/vapor/encryptedfile"
	"github.com/docopt/docopt-go"
)

const usage = `encryptedfile

Usage:
  encryptedfile write <filename>
  encryptedfile read <filename>`

var key = [32]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
	'0', '1'}

func main() {
	args, _ := docopt.Parse(usage, nil, true, "V0", false)
	if args["write"].(bool) {
		filename := args["<filename>"].(string)
		size := 4096*10 + 500
		toWrite := make([]byte, size)
		toRead := make([]byte, size)
		_, err := io.ReadFull(rand.Reader, toWrite[:])

		f, err := encryptedfile.Open(filename, key)
		defer f.Close()

		if err != nil {
			log.Fatal(err)
		}
		_, err = f.WriteAt(toWrite, 0)
		if err != nil {
			log.Fatal(err)
		}
		_, err = f.ReadAt(toRead, 0)

		if bytes.Equal(toWrite, toRead) {
			fmt.Printf("YUP")
		} else {
			fmt.Printf("NOPE")
		}
	}
	if args["read"].(bool) {
		filename := args["<filename>"].(string)
		f, err := encryptedfile.Open(filename, key)
		if err != nil {
			log.Fatal(err)
		}
		bytes := make([]byte, 100)
		_, err = f.ReadAt(bytes, 0)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s", bytes)
	}
}
