package main

import (
	"fmt"
	"os"
	"log"

	"github.com/colinmarc/hdfs"
)

func main() {
	args := os.Args

	if (len(args) > 2) {
		fmt.Println("Too many arguments")
		os.Exit(1)
	} else if (len(args) == 1) {
		args = []string{args[0], "help"}
	}

	if (args[1] == "gen" || args[1] == "-generate") {
		generate()
	} else if (args[1] == "cl" || args[1] == "-clean") {
		cleanTestFolder("/test")
	} else {
		if (args[1] != "help" && args[1] != "-help") {
			fmt.Println("Unknown command: " + args[1])
		}
		fmt.Println("Usage: " + args[0] + " COMMAND\n" +
			"\nValid commands:\n  gen or -generate     to generate data in test directory of hdfs\n  cl or -clean         to clean test directory")
	}
}

func generate() {
	generateFiles()
	generateDirectories()
}

func generateFiles() {
	client, _ := hdfs.New("localhost:54310")
	client.CreateEmptyFile("/test/test1.txt")
	fileWriter, _ := client.Create("/test/test2.txt")
	fileWriter.Write([]byte("random"))
	fileWriter.Close()
}

func generateDirectories() {
	client, _ := hdfs.New("localhost:54310")
	client.Mkdir("/test/testme", 0777)
	client.Mkdir("/test/testhim", 0777)

	client.CreateEmptyFile("/test/testme/test3.txt")
}

func cleanTestFolder(root string) {

	client, _ := hdfs.New("localhost:54310")
	osPaths, _ := client.ReadDir(root)
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
			client.Remove(p)
			fmt.Printf("%s is removed\n", p)
		}

		for _, dir := range dirs {
			cleanTestFolder(dir)
			client.Remove(dir)
			fmt.Printf("%s is removed\n", dir)
		}
	}
}