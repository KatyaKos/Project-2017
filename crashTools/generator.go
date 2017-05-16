package crashTools

import (
	"fmt"
	"os"
	"strconv"
	"Project-2017/crashTools/utils/crashTools"
	"github.com/colinmarc/hdfs"
)

func build_file_path(dir_path string, file_name string) string {
	return dir_path + "/" + file_name
}

func hdfs_mkdir(dir_path string, perm os.FileMode) {
	err := client.Mkdir(dir_path, perm)
	PrintErrorToFmtAndExit(err)
	//fmt.Printf("%s is generated\n", dir_path)
}

func hdfs_touch(file_path string) {
	err := client.CreateEmptyFile(file_path)
	PrintErrorToFmtAndExit(err)
	//fmt.Printf("%s is generated\n", file_path)
}

func hdfs_create_file(file_path string, content string) {
	fileWriter, err := client.Create(file_path)
	PrintErrorToFmtAndExit(err)
	fileWriter.Write([]byte(content))
	fileWriter.Close()
	//fmt.Printf("%s is generated\n", file_path)
}

func is_empty(dir_path string) bool {
	summary, err := client.GetContentSummary(dir_path)
	PrintErrorToFmtAndExit(err)
	return (summary.Size() == 0)
}

//type 1
func generate_empty_files_in_folder(root string, n int) {
	for i := 0; i < n; i++ {
		hdfs_touch(build_file_path(root, "f" + strconv.Itoa(i)))
	}
}

//type 2
func generate_empty_directories(root string, n int) {
	for i := 0; i < n; i++ {
		hdfs_mkdir(build_file_path(root, "d" + strconv.Itoa(i)), 777)
	}
}

//type 3
func generate_empty_subdirectories(root string, n int) {
	for i := 0; i < n; i++ {
		root = build_file_path(root, "d" + strconv.Itoa(i))
		hdfs_mkdir(root, 777)
	}
}

//type 4
func generate_files_in_folder(root string, n int, file_length int) {
	for i := 0; i < n; i++ {
		content := rand.String(rand.Int(file_length))
		hdfs_create_file(build_file_path(root, "f" + strconv.Itoa(i)), content)
	}
}

//type 5
func randomized_generator(root string, depth int, folder_size int, file_length int) {
	queue := make([]string, 0)
	queue = append(queue, root)

	for i := 0; i < depth; i++ {
		l := len(queue)
		for l > 0 {
			path := queue[0]
			queue = queue[1:]
			l -= 1
			n := rand.Int(folder_size)
			m := rand.Int(n)
			generate_files_in_folder(path, m, file_length)
			generate_empty_directories(path, n - m)
			for j := 0; j < n - m; j++ {
				queue = append(queue, build_file_path(path, "d" + strconv.Itoa(j)))
			}
		}
	}
}

var client *hdfs.Client

func Generate(parsedArgs ParsedArguments) {
	root := parsedArgs.Root
	var err error
	client, err = hdfs.New(parsedArgs.Namenode)
	PrintErrorToFmtAndExit(err)

	if !is_empty(root) {
		fmt.Fprintln(os.Stderr, "The directory is not empty. Please, use \"cl\" command first.")
		os.Exit(1)
	}
	fmt.Println("Starting generating...")

	var dir_path string
	//Generated 1000000 empty files in one directory. Successfully finished.
	//
	//dir_path = build_file_path(root, "testType1")
	//hdfs_mkdir(dir_path, 777)
	//generate_empty_files_in_folder(dir_path, 1000000)

	//Generated 1000 empty folders in one directory.
	//
	//dir_path = build_file_path(root, "testType2")
	//hdfs_mkdir(dir_path, 777)
	//generate_empty_directories(dir_path, 1000)

	//Generated 1000 subdirectories (one directory contains only it's subdirectory). Stopped on the 1000's directory with error:
	//mkdir /test/testType2/0/1/2/3/4/5/ ... /995/996/997/998: mkdirs call failed with ERROR_APPLICATION (java.io.IOException)"
	//
	//dir_path = build_file_path(root, "testType3")
	//hdfs_mkdir(dir_path, 777)
	//generate_empty_subdirectories(dir_path, 1000)

	//Generated 10000 files in one folder. Files have random content (string, length not more than 1000000 characters).
	//Successfully finished.
	//
	//dir_path = build_file_path(root, "testType4")
	//hdfs_mkdir(dir_path, 777)
	//generate_files_in_folder(dir_path, 10000, 1000000)

	//Generated random hierarcy of files and folders. Max depth = 100. Max folder size = 100. Max file length = 1000000.
	//
	//dir_path = build_file_path(root, "testType5")
	//hdfs_mkdir(dir_path, 777)
	//randomized_generator(dir_path, 100, 100, 1000000)
}