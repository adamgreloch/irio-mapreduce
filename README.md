# edi-mapreduce

## Assumptions

Client provides the following binaries:

### Map binary
```
./map -i <input_file_path> -o <output_file_path>
```
Behaviour: reads from input file and forms a list of keys 
the output file specified by path

### Partitioner
```
./partition -R <R-param> -i <input_file_path> -o <output_directory>
```
Behaviour: deterministically partitions the input file into R partitions

### Reduce
```
./reduce -i <input_file_path> -o <output_file_path>
```
Behaviour: reduces (key, list(value)) into (key, new_value)

Input files does not need to be sorted, but there is a guarantee that 
all values for a given key are in the input file.


## gRPC/protobuf workflow

To generate Java stubs based on `src/main/proto` files, invoke

```
./gradlew generateProto
```

Service stubs (`ImplBase` classes) will be located
under `/build/generated/main/grpc/`
