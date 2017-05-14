package crashTools

import (
	"fmt"
	"github.com/colinmarc/hdfs"
)

func CleanHdfsFolder(root string, parsedArgs ParsedArguments) {
	namenode := parsedArgs.Namenode
	client, err := hdfs.New(namenode)
	PrintErrorToFmtAndExit(err)
	osPaths, err := client.ReadDir(root)
	PrintErrorToFmtAndExit(err)
	paths := make([]string, 0, len(osPaths))

	for _, p := range osPaths {
		paths = append(paths, root+"/"+p.Name())
	}

	if len(paths) == 0 {
		return
	}

	files := make([]string, 0, len(paths))
	dirs := make([]string, 0, len(paths))

	for _, p := range paths {
		fi, err := client.Stat(p)
		PrintErrorToFmtAndExit(err)

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
			PrintErrorToFmtAndExit(err)
			fmt.Printf("%s is removed\n", p)
		}

		for _, dir := range dirs {
			CleanHdfsFolder(dir, parsedArgs)
			err = client.Remove(dir)
			PrintErrorToFmtAndExit(err)
			fmt.Printf("%s is removed\n", dir)
		}
	}
}