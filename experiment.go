package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	fmt.Println("Start!!")

	_, err := os.MkdirTemp("", "badger")
	if err != nil {
		log.Fatal(err)
	}
}
