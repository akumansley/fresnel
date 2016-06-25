package main

import (
	"fmt"
	"log"

	"github.com/awans/fresnel/encryptedfile"
	"github.com/docopt/docopt-go"
	"github.com/steveyen/gkvlite"
)

const usage = `eg

Usage:
  eg write <filename> <key> <val>
  eg read <filename> <key>`

var encKey = [32]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
	'0', '1'}

func main() {
	args, _ := docopt.Parse(usage, nil, true, "V0", false)
	if args["write"].(bool) {
		filename := args["<filename>"].(string)
		key := []byte(args["<key>"].(string))
		val := []byte(args["<val>"].(string))
		f, err := encryptedfile.Open(filename, encKey)
		defer f.Close()

		s, err := gkvlite.NewStore(f)
		if err != nil {
			log.Fatal(err)
		}
		c := s.SetCollection("tests", nil)
		fmt.Printf("SET: %s, %s\n", key, val)
		c.Set(key, val)
		s.Flush()
	}
	if args["read"].(bool) {
		filename := args["<filename>"].(string)
		key := []byte(args["<key>"].(string))
		f, err := encryptedfile.Open(filename, encKey)
		if err != nil {
			log.Fatal(err)
		}
		s, err := gkvlite.NewStore(f)
		if err != nil {
			log.Fatal(err)
		}
		c := s.SetCollection("tests", nil)
		val, err := c.Get(key)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("GET: %s\n", val)
	}
}
