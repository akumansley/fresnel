package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

	_ "github.com/awans/vapor/ekv"
	"github.com/blevesearch/bleve"
	"github.com/docopt/docopt-go"
)

const usage = `ekv

Usage:
  ekv index <json_file>
	ekv search <query>
	ekv clean`

const indexDir = "index"
var pad = []byte("lorem ipsum dolor sit aaodijawoidjawdijaowdijaowidjmet blah blah blah blah")
const keySize = 32
var config = map[string]interface{}{"key": pad}

func createOrGetIndex(id string) (bleve.Index, error) {
	p := path.Join(indexDir, id)
	index, err := bleve.OpenUsing(p, config)
	if err == nil {
		return index, nil
	}
	mapping := bleve.NewIndexMapping()

	index, err = bleve.NewUsing(p, mapping, "upside_down", "ekv", config)
	if err != nil {
		return nil, err
	}
	return index, nil
}

var providerIDs = []string{"1", "2"}

func main() {
	args, _ := docopt.Parse(usage, nil, true, "V0", false)
	if args["index"].(bool) {
		filename := args["<json_file>"].(string)
		bytes, err := ioutil.ReadFile(filename)
		var jsonList []interface{}

		err = json.Unmarshal(bytes, &jsonList)
		if err != nil {
			log.Fatal(err)
		}

		indexMap := make(map[string]bleve.Index)
		for _, providerID := range providerIDs {
			index, err := createOrGetIndex(providerID)
			if err != nil {
				log.Fatal(err)
			}
			indexMap[providerID] = index
		}

		for _, obj := range jsonList {
			doc := obj.(map[string]interface{})
			providerID := doc["provider_id"].(string)
			docID := doc["id"].(string)
			index := indexMap[providerID]
			index.Index(docID, doc)
		}
		for _, ix := range indexMap {
			ix.Close()
		}
	}

	if args["search"].(bool) {
		q := args["<query>"].(string)

		mergedIndex := bleve.NewIndexAlias()
		for _, providerID := range providerIDs {
			p := path.Join(indexDir, providerID)
			index, err := bleve.OpenUsing(p, config)
			if err != nil {
				log.Fatal(err)
			}
			mergedIndex.Add(index)
		}
		query := bleve.NewMatchQuery(q)
		search := bleve.NewSearchRequest(query)
		hits, err := mergedIndex.Search(search)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(hits)
	}
	if args["clean"].(bool) {
		for _, providerID := range providerIDs {
			p := path.Join(indexDir, providerID)
			err := os.RemoveAll(p)
			if err != nil {
				log.Fatal(err)
			}
		}

	}
}
