package main

import (
	"fmt"
	"os"
	"flag"
	"github.com/colinmarc/hdfs"
)

type ParsedArguments struct {
	Path string
	Cmd string
	Namenode string
	Root string
}

func parse_options() {
	flag.StringVar(&parsedArgs.Namenode, "nn", "localhost:54310", "your namenode name")
	flag.StringVar(&parsedArgs.Root, "r", "/test", "directory you want to work with")
}

func parse_command(path string, args []string) {
	parsedArgs.Path = path

	if (len(args) > 1) {
		fmt.Fprintln(os.Stderr, "Too many commands")
		os.Exit(1)
	}
	if (len(args) == 0 || args[0] == "help") {
		print_help(path)
		os.Exit(0)
	}
	if (args[0] == "gen" || args[0] == "generate") {
		parsedArgs.Cmd = "gen"
	} else if (args[0] == "cl" || args[0] == "clean") {
		parsedArgs.Cmd = "cl"
	} else {
		fmt.Fprintln(os.Stderr, "UNKNOWN COMMAND: " + args[0])
		print_help(path)
		os.Exit(1)
	}
} 

func print_help(path string) {
	fmt.Printf("Usage: %s -option_name=option_value COMMAND\n\nValis commands:\n", path)
	fmt.Println("  generate [gen] --root [-r] --namenode [-nn]     to generate data in root")
	fmt.Println("  clean [cl] --root [-r] --namenode [-nn]         to clean root from data")
}

func check_arguments() {
	namenode := parsedArgs.Namenode
	root := parsedArgs.Root
	client, err := hdfs.New(namenode)
	print_error_to_fmt(err)
	finfo, err := client.Stat(root)
	print_error_to_fmt(err)
	if (!finfo.IsDir()) {
		fmt.Fprintf(os.Stderr, "Please, provide the directory name in \"root\" option. %s is not directory!\n", parsedArgs.Root)
		os.Exit(1)
	}
}

var parsedArgs ParsedArguments

func main() {
	parse_options()
	flag.Parse()
	parse_command(os.Args[0], flag.Args())
	check_arguments()

	if (parsedArgs.Cmd == "gen" || parsedArgs.Cmd == "generate") {
		generate_test_data()
	} else if (parsedArgs.Cmd == "cl" || parsedArgs.Cmd == "clean"){
		clean_test_data(parsedArgs.Root)
	}
}

func print_error_to_fmt(err error) {
	if (err != nil) {
		fmt.Fprintf(os.Stderr, "ERROR! \"%v\"\n", err)
		os.Exit(1)
	}
}

func IsEmpty(client *hdfs.Client, root string) bool {
	summary, err := client.GetContentSummary(root)
	print_error_to_fmt(err)
	return (summary.Size() == 0)
}

func empty_directory(client *hdfs.Client, root string) {
	var isContinue string
	fmt.Printf("The directory is not empty. It will be cleaned. Do you want to continue? Y/N  ")
	fmt.Scanf("%s", &isContinue)
	if (isContinue != "Y") {
		os.Exit(0)
	}
	clean_test_data(root)
}

func build_file_name(root string, name string) string {
	return root + "/" + name
}

func generate_directory(client *hdfs.Client, root string, perm os.FileMode) {
	err := client.Mkdir(root, perm)
	print_error_to_fmt(err)
	fmt.Printf("%s is generated\n", root)
}

func generate_empty_file(client *hdfs.Client, root string) {
	err := client.CreateEmptyFile(root)
	print_error_to_fmt(err)
	fmt.Printf("%s is generated\n", root)
}

func generate_file(client *hdfs.Client, root string, content string) {
	fileWriter, err := client.Create(root)
	print_error_to_fmt(err)
	fileWriter.Write([]byte(content))
	fileWriter.Close()
	fmt.Printf("%s is generated\n", root)
}

func generate_test_data() {
	namenode := parsedArgs.Namenode
	root := parsedArgs.Root
	client, err := hdfs.New(namenode)
	print_error_to_fmt(err)

	if (!IsEmpty(client, root)) {
		empty_directory(client, root)
		fmt.Println("\n")
	}
	fmt.Println("Starting generating...")

	generate_directory(client, build_file_name(root, "testme"), 077)
	generate_directory(client, build_file_name(root, "testhim"), 077)
	generate_empty_file(client, build_file_name(root, "testme/test3.txt"))
	generate_empty_file(client, build_file_name(root, "test1.txt"))
	generate_file(client, build_file_name(root, "test2.txt"), "random data")
}

func clean_test_data(root string) {
	namenode := parsedArgs.Namenode
	client, err := hdfs.New(namenode)
	print_error_to_fmt(err)
	osPaths, err := client.ReadDir(root)
	print_error_to_fmt(err)
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
		print_error_to_fmt(err)

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
			print_error_to_fmt(err)
			fmt.Printf("%s is removed\n", p)
		}

		for _, dir := range dirs {
			clean_test_data(dir)
			err = client.Remove(dir)
			print_error_to_fmt(err)
			fmt.Printf("%s is removed\n", dir)
		}
	}
}