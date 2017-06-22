package crashTools

import (
	"flag"
	"os"
)

type ParsedArguments struct {
	Path     string
	Cmd      string
	Namenode string
	Root     string
	Type     string
}

func Parse(arguments []string) ParsedArguments {
	parsedArgs := ParsedArguments{Path: arguments[0]}

	cleanCommand := flag.NewFlagSet("cl", flag.ExitOnError)
	genCommand := flag.NewFlagSet("gen", flag.ExitOnError)

	cleanRootPtr := cleanCommand.String("root", "/test", "directory you want to clean")
	cleanNamenodePtr := cleanCommand.String("namenode", "master:54310", "namenode with the directory")
	genRootPtr := genCommand.String("root", "/test", "directory where you want to generate data")
	genNamenodePtr := genCommand.String("namenode", "master:54310", "namenode with the directory")
	genTypePtr := genCommand.String("type", "5", "type of generation")

	if len(arguments) < 2 {
		PrintHelp(arguments[0])
		os.Exit(1)
	}

	switch arguments[1] {
	case "gen":
		genCommand.Parse(arguments[2:])
	case "cl":
		cleanCommand.Parse(arguments[2:])
	default:
		PrintHelp(arguments[0])
		os.Exit(1)
	}

	if cleanCommand.Parsed() {
		parsedArgs.Cmd = "cl"
		parsedArgs.Root = *cleanRootPtr
		parsedArgs.Namenode = *cleanNamenodePtr
	} else if genCommand.Parsed() {
		parsedArgs.Cmd = "gen"
		parsedArgs.Root = *genRootPtr
		parsedArgs.Namenode = *genNamenodePtr
		parsedArgs.Type = *genTypePtr
	}

	return parsedArgs
}