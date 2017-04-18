package main

import (
	"fmt"
	"os"
	"log"

	"github.com/colinmarc/hdfs"
)

func main() {
	args := os.Args

	if (len(args) > 6) {
		fmt.Println("Too many arguments")
		os.Exit(1)
	} else if (len(args) == 1) {
		args = []string{args[0], "help"}
	}

	namenode := "localhost:54310"
	root := "/test"
	if (len(args) >= 3) {
		if (len(args) < 4) {
			fmt.Printf("Please, provide value for option \"%s\"\n", args[2])
			os.Exit(1)
		}
		if (args[2] == "-nn" || args[2] == "--namenode") {
			namenode = args[3]
		} else if (args[2] == "-r" || args[2] == "--root") {
			root = args[3]
		} else {
			fmt.Println("Unknown option")
			os.Exit(1)
		}
	}
	if (len(args) >= 5) {
		if (len(args) < 6) {
			fmt.Printf("Please, provide value for option \"%s\"\n", args[4])
			os.Exit(1)
		}
		if (args[4] == "-nn" || args[4] == "--namenode") {
			namenode = args[5]
		} else if (args[4] == "-r" || args[4] == "--root") {
			root = args[5]
		} else {
			fmt.Println("Unknown option")
			os.Exit(1)
		}
	}


	if (args[1] == "gen" || args[1] == "generate") {
		generateTestData(namenode, root)
	} else if (args[1] == "cl" || args[1] == "clean"){
		cleanTestData(namenode, root)
	} else {
		if (args[1] != "help") {
			fmt.Println("Unknown command: " + args[1])
		}
		fmt.Printf("Usafe: %s COMMAND option_name option_value\n\nValis commands:\n", args[0])
		fmt.Println("  generate [gen] --root [-r] --namenode [-nn]     to generate data in root")
		fmt.Println("  clean [cl] --root [-r] --namenode [-nn]         to clean root from data")
	}
}

func generateTestData(namenode string, root string) {
	client, err := hdfs.New(namenode)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Mkdir(root + "/testme", 0777)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Mkdir(root + "/testhim", 0777)
	if err != nil {
		log.Fatal(err)
	}

	err = client.CreateEmptyFile(root + "/testme/test3.txt")
	if err != nil {
		log.Fatal(err)
	}

	err = client.CreateEmptyFile(root + "/test1.txt")
	if err != nil {
		log.Fatal(err)
	}
	fileWriter, err := client.Create(root + "/test2.txt")
	if err != nil {
		log.Fatal(err)
	}
	fileWriter.Write([]byte("random"))
	fileWriter.Close()
}

func cleanTestData(namenode string, root string) {

	client, err := hdfs.New(namenode)
	if err != nil {
		log.Fatal(err)
	}
	osPaths, err := client.ReadDir(root)
	if err != nil {
		log.Fatal(err)
	}
	paths := make([]string, 0, len(osPaths))

	for _, p := range osPaths {
		paths = append(paths, root + "/" + p.Name())
	}

	if len(paths) == 0 {
		return
	}

	files := make([]string, 0, len(paths))
	dirs := make([]string, 0, len(paths))

	for _, p := range paths {
		fi, err := client.Stat(p)
		if err != nil {
			log.Fatal(err)
		}

		if fi.IsDir() {
			dirs = append(dirs, p)
		} else {
			files = append(files, p)
		}
	}

	if len(files) == 0 && len(dirs) == 1 {
		fmt.Println("Finished cleaning!")
	} else {
		for _, p := range files {
			err = client.Remove(p)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s is removed\n", p)
		}

		for _, dir := range dirs {
			cleanTestData(namenode, dir)
			err = client.Remove(dir)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s is removed\n", dir)
		}
	}
}