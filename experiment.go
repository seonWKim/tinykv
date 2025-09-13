package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Connor1996/badger"
)

func main() {
	fmt.Println("Start!!")

	dir, err := os.MkdirTemp("", "badger")
	if err != nil {
		log.Fatal(err)
	}

	defer os.RemoveAll(dir)
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Creating transaction")
	txn := db.NewTransaction(true)

	fmt.Println("Set key")
	err = txn.Set([]byte("key"), []byte("value"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Commit")
	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Viewing...")
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("key"))
		fmt.Println("item: ", item)
		fmt.Println(err)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
