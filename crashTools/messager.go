package crashTools

import (
	"fmt"
	"os"
)

func PrintHelp(path string) {
	fmt.Printf("Usage: %s COMMAND -option_name=option_value\n\nValid commands:\n", path)
	fmt.Println("  gen -root  -namenode  -type         to generate data in root")
	fmt.Println("  cl  -root  -namenode                to clean root from data")
	fmt.Println("\n\nTypes of generation:")
	fmt.Println("  1: epmty files in the root directory")
	fmt.Println("  2: empty folders in the root directory")
	fmt.Println("  3: branch of empty subfolders in the root directory")
	fmt.Println("  4: files with random content in the root directory")
	fmt.Println("  5: random hierarcy of folders and files in the root directory")
	fmt.Println("  6: replicated files with random content in the root directory")
	fmt.Println("  7: copies of local file in the root directory.")
}

func PrintErrorToFmtAndExit(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR! \"%v\"\n", err)
		os.Exit(1)
	}
}