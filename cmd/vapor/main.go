package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

	_ "github.com/awans/vapor/encryptedkv"
	"github.com/blevesearch/bleve"
	"github.com/docopt/docopt-go"
)

const usage = `vape

Usage:
  vapor index <json_file>
	vapor search <query>
	vapor clean`

const indexDir = "index"

func createOrGetIndex(id string) (bleve.Index, error) {
	p := path.Join(indexDir, id)
	index, err := bleve.Open(p)
	if err == nil {
		return index, nil
	}
	mapping := bleve.NewIndexMapping()
	index, err = bleve.NewUsing(p, mapping, "upside_down", "encryptedkv", make(map[string]interface{}))
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
	}

	if args["search"].(bool) {
		q := args["<query>"].(string)

		mergedIndex := bleve.NewIndexAlias()
		for _, providerID := range providerIDs {
			p := path.Join(indexDir, providerID)
			index, err := bleve.Open(p)
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
