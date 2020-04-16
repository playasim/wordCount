#!/usr/bin/env bash

mvn clean package
java -cp target/wordCount-1.0-SNAPSHOT-jar-with-dependencies.jar com.softnero.wordCount
java -cp target/wordCount-1.0-SNAPSHOT-jar-with-dependencies.jar com.softnero.lineMedian