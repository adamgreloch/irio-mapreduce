# edi-mapreduce

## gRPC/protobuf workflow

To generate Java stubs based on `src/main/proto` files, invoke

```
./gradlew generateProto
```

Service stubs (`ImplBase` classes) will be located
under `/build/generated/main/grpc/`
