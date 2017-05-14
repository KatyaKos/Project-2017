package crashTools

import (
	"fmt"
	"os"
	"github.com/colinmarc/hdfs"
)

func build_file_path(dir_path string, file_name string) string {
	return dir_path + "/" + file_name
}

func hdfs_mkdir(client *hdfs.Client, dir_path string, perm os.FileMode) {
	err := client.Mkdir(dir_path, perm)
	PrintErrorToFmtAndExit(err)
	fmt.Printf("%s is generated\n", dir_path)
}

func hdfs_touch(client *hdfs.Client, root string) {
	err := client.CreateEmptyFile(root)
	PrintErrorToFmtAndExit(err)
	fmt.Printf("%s is generated\n", root)
}

func hdfs_create_file(client *hdfs.Client, root string, content string) {
	fileWriter, err := client.Create(root)
	PrintErrorToFmtAndExit(err)
	fileWriter.Write([]byte(content))
	fileWriter.Close()
	fmt.Printf("%s is generated\n", root)
}

func is_empty(client *hdfs.Client, root string) bool {
	summary, err := client.GetContentSummary(root)
	PrintErrorToFmtAndExit(err)
	return (summary.Size() == 0)
}

func Generate(parsedArgs ParsedArguments) {
	namenode := parsedArgs.Namenode
	root := parsedArgs.Root
	client, err := hdfs.New(namenode)
	PrintErrorToFmtAndExit(err)

	if !is_empty(client, root) {
		fmt.Fprintln(os.Stderr, "The directory is not empty. Please, use \"cl\" command first.")
		os.Exit(1)
	}
	fmt.Println("Starting generating...")

	hdfs_mkdir(client, build_file_path(root, "testme"), 077)
	hdfs_mkdir(client, build_file_path(root, "testhim"), 077)
	hdfs_touch(client, build_file_path(root, "testme/test3.txt"))
	hdfs_touch(client, build_file_path(root, "test1.txt"))
	hdfs_create_file(client, build_file_path(root, "test2.txt"), "random data")
}