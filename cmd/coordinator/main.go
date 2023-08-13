package main

import (
	"bufio"
	"flag"
	"log"
	"mapreduce/internal/coordinator"
	"os"
)

func main() {
	inputFilePtr := flag.String("f", "", "File that contains the names of input files.")
	reduceCountPtr := flag.Int("r", -1, "Number of reduce tasks.")
	portPtr := flag.String("p", "3000", "Port of the coordinator server.")

	flag.Parse()

	if *inputFilePtr == "" {
		log.Fatalln("Remember to use the -f=FILE_NAME flag to specify the file that contains the names of input files.")
	}

	if *reduceCountPtr == -1 {
		log.Fatalln("Remember to use the -r=NUMBER flag to specify the number of reduce tasks.")
	}

	file, err := os.Open(*inputFilePtr)
	if err != nil {
		log.Fatalln("Failed to open file containing names of input files.")
	}

	inputFiles := []string{}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		inputFiles = append(inputFiles, scanner.Text())
	}

	coordinator.CreateCoordinator(inputFiles, *reduceCountPtr, *portPtr)

	select {}
}
