# Service support for java

Core stuff for java services and gRPC endpoints. The gRPC proto files are located in submodule [proto](./src/main/proto). 
The base gRPC service interfaces are included with the jar file.

## Cloning
This project links to a protobuf submodule. After cloning this project you must run:
```
git submodule init
git submodule update
```

Alternatively you can do this when you clone:
```
git clone --recursive https://github.com/marbles-ai/mservice-java.git
```

## Building the jar
The jar file will be located at `./build/libs`. Build the jar by running:
```
gradle build
```

## Uploading jar to Github

Jars are stored on our github repository. 
Make sure you tag with the version each time you upload.

You can include Github jars in another maven or gradle project using [jitpack](https://jitpack.io/).

## TODO
1. Add a script to increment version, tag, and upload jar.

