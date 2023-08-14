# mapreduce

Command line tool for MapReduce functions written as Go plugins. Workers can be run on the same machine or different machines or both. Coordinator and workers communicate through RPC.

### Usage
Requires Go to be installed.
1. Build the coordinator and worker.
    ```
    go build ./cmd/coordinator
    go build ./cmd/worker
    ```

2. Define input files in a file with the path of each input file on each line.
    ```
    fileA.in
    fileB.in
    fileC.in
    fileD.in
    fileE.in
    ```

3. Write the Map and Reduce functions in a separate .go file. They must have the following type signatures. structs.KeyValue should be imported from "mapreduce/internal/structs".
    ```
    func Map(filename string, contents string) []structs.KeyValue
    func Reduce(key string, values []string) string
    ```

4. Compile the Map and Reduce functions as a Go plugin.
    ```
    go build -buildmode=plugin mapreduce.go
    ```

5. Start the coordinator.
    ```
    ./coordinator -f=FILE -r=NUMBER -p=PORT
    ```
    -f=FILE: Path of the file containing the input file paths.
    -r=NUMBER: Number of reduce tasks.
    -p=PORT: Port to listen on for worker requests.

6. Start multiple workers.
    ```
    ./worker -f=PLUGIN -c=ADDRESS -p=PORT
    ```
    -f=PLUGIN: Path to the compiled Go plugin. Should be a *.so file.
    -c=ADDRESS: Address of the coordinator.
    -p=PORT: Port of the coordinator.

7. Wait for the workers to finish. Output files will be written to the local directory as `output-*`.
