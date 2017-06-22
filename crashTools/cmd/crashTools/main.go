package main

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"os"
	"Project-2017/crashTools"
)

func check_arguments() {
	namenode := parsedArgs.Namenode
	root := parsedArgs.Root
	client, err := hdfs.New(namenode)
	crashTools.PrintErrorToFmtAndExit(err)
	finfo, err := client.Stat(root)
	crashTools.PrintErrorToFmtAndExit(err)
	if !finfo.IsDir() {
		fmt.Fprintf(os.Stderr, "Please, provide the directory name in \"root\" option. %s is not directory!\n", parsedArgs.Root)
		os.Exit(1)
	}
}

var parsedArgs crashTools.ParsedArguments

func main() {
	parsedArgs = crashTools.Parse(os.Args)
	check_arguments()

	if parsedArgs.Cmd == "gen" {
		crashTools.Generate(parsedArgs)
	} else if parsedArgs.Cmd == "cl" {
		crashTools.CleanHdfsFolder(parsedArgs.Root, parsedArgs)
	}
}