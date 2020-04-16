package com.softnero;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class wordCount {

    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf().setAppName("word_count").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("wc_input");
        BufferedWriter writer = new BufferedWriter(new FileWriter("wc_output/result.txt"));
        JavaRDD<String> wordmap = lines
                .map(line -> {
                    StringBuilder newline = new StringBuilder();
                    for(int i = 0; i < line.length(); i++){
                        char tempchar = line.charAt(i);
                        if((tempchar <= 'z' && tempchar >= 'a') || (tempchar == ' ' || tempchar <= '9' && tempchar >= '0')){
                            newline.append(tempchar);
                        }else if(tempchar <= 'Z' && tempchar >= 'A'){
                            tempchar = (char)(tempchar - 'A' + 'a');
                            newline.append(tempchar);
                        }
                    }
                    return newline.toString();
                })
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts = wordmap.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey(Integer::sum);
        counts.collectAsMap().forEach((k,v) -> {
            try {
                writer.write(k + " " + v.toString() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        writer.close();
    }



}
