#!/bin/bash

# Make build directory
if [[ ! -d build ]]; then
	mkdir build
fi

# Get dependencies
if [[ ! -f build/java_websocket.jar ]]; then
	wget http://central.maven.org/maven2/org/java-websocket/Java-WebSocket/1.3.4/Java-WebSocket-1.3.4.jar -O build/java_websocket.jar
fi

# Compiling files
javac -cp build/java_websocket.jar -Xlint:unchecked -d build/ $(find . -name "*.java" -type f)

# Package into jar
cd build/
jar cvf gambezi.jar com/*
