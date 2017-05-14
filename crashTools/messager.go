package crashTools

import (
	"fmt"
	"os"
)

func PrintHelp(path string) {
	fmt.Printf("Usage: %s COMMAND -option_name=option_value\n\nValid commands:\n", path)
	fmt.Println("  gen -root -namenode          to generate data in root")
	fmt.Println("  cl  -root  -namenode         to clean root from data")
}

func PrintErrorToFmtAndExit(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR! \"%v\"\n", err)
		os.Exit(1)
	}
}