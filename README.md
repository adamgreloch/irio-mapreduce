# edi-mapreduce

## Assumptions

Client provides the following binaries:

### Map binary
```
./map -i <input_file_path> -o <output_file_path>
```
Behaviour: forms a list of pairs (key, some value) based on the input file
contents and saves it to the output file specified by path

### Partitioner
```
./partition -R <R-param> -i <input_file_path> -o <output_directory>
```
Behaviour: deterministically partitions the input file into R partitions

### Reduce
```
./reduce -i <input_file_path> -o <output_file_path>
```
Behaviour: reduces all (k, value) pairs for k = key into one (key, new_value)
where new_value is an aggregate of all values from pairs.

Input files do not need to be sorted, but there is a guarantee that 
all values for a given key are in the input file.

## Build

### gRPC

To generate Java stubs based on `src/main/proto` files, invoke

```
./gradlew generateProto
```

Service stubs (`ImplBase` classes) will be located
under `/build/generated/main/grpc/`

### Pushing Docker images to Artifact Registry

https://cloud.google.com/artifact-registry/docs/docker/authentication
