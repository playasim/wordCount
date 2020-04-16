package com.softnero;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class lineMedian {
    static List<Double> medians = new ArrayList<>();
    static List<Integer> list = new ArrayList<>();
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("median").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("wc_input");

        lines.map(t -> t.split(" ").length)
                .foreach(count -> {
                    list.add(count);
                    medianCount(list);
                });
        writeInfoFiles("wc_output/med_result.txt");
    }

    private static void writeInfoFiles(String path) throws Exception{
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(path));
            for (Double d : medians) {
                writer.write(d.toString() + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            writer.close();
        }
    }
    public static void medianCount(List<Integer> list){

        Collections.sort(list);
        int len = list.size();
        if(len % 2 == 0 ) {
            medians.add(((double)list.get(len / 2 - 1) + (double)list.get(len / 2 )) / 2);
        }else {
            medians.add((double)list.get((len - 1) / 2));
        }
    }
}
