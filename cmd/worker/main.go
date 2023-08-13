package main

import (
	"flag"
	"log"
	"mapreduce/internal/structs"
	"mapreduce/internal/worker"
	"plugin"
)

func main() {
	pluginFilePtr := flag.String("f", "", "Go plugin file that contains the Map and Reduce functions.")
	coordinatorAddressPtr := flag.String("c", "", "Address of the coordinator server.")
	coordinatorPortPtr := flag.String("p", "", "Port of the coordinator server.")

	flag.Parse()

	if *pluginFilePtr == "" {
		log.Fatalln("Remember to use the flag -f=FILE_NAME to specify the Go plugin file.")
	}

	if *coordinatorAddressPtr == "" {
		log.Fatalln("Missing coordinator address. [-c=ADDRESS]")
	}

	if *coordinatorPortPtr == "" {
		log.Fatalln("Missing coordinator port. [-p=PORT]")
	}

	pluginFile, err := plugin.Open(*pluginFilePtr)
	if err != nil {
		log.Fatalln("Failed to load plugin file.")
	}

	mapFunctionSymbol, err := pluginFile.Lookup("Map")
	if err != nil {
		log.Fatalln("Failed to load Map function from plugin file.")
	}

	reduceFunctionSymbol, err := pluginFile.Lookup("Reduce")
	if err != nil {
		log.Fatalln("Failed to load Reduce function from plugin file.")
	}

	mapFunction := mapFunctionSymbol.(func(string, string) []structs.KeyValue)
	reduceFunction := reduceFunctionSymbol.(func(string, []string) string)

	w := worker.CreateWorker(*coordinatorAddressPtr, *coordinatorPortPtr, mapFunction, reduceFunction)
	w.Start()
}
