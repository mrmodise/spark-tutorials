package com.mrmodise;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class WordCount {

    public static void main(String[] args) {

        // start Apache Spark session
        SparkSession spark = SparkSession
                .builder()
                .config("spark.master", "local")
                .appName("Word Counter")
                .getOrCreate();

        // remove unnecessary logger messages displayed when application is running
        spark.sparkContext().setLogLevel("ERROR");

        // read and cache the READMe.md file passed as runtime argument. (See resources folder)
        Dataset<String> logData = spark
                .read()
                .textFile(args[0])
                .cache();

        // count the number of A's in the file
        long numAs = logData
                .filter(s -> s.contains("a"))
                .count();

        // count the number of B's in the file
        long numBs = logData
                .filter(s -> s.contains("b"))
                .count();

        // print results to console
        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        // stop Apache spark session
        spark.stop();

    }
}
