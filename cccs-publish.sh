#!/bin/bash

mkdir -p build/jars
cp ./spark/build/libs/*.jar build/jars
cp ./spark/v3.5/spark/build/libs/*.jar build/jars
cp ./spark/v3.5/spark-extensions/build/libs/*.jar build/jars
cp ./spark/v3.5/spark-runtime/build/libs/*.jar build/jars