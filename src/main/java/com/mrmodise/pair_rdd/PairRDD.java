package com.mrmodise.pair_rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;

public class PairRDD {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Java Pair RDD")
                .setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> tuple = Arrays.asList(
                new Tuple2<>("Lily", 23),
                new Tuple2<>("Jack", 20),
                new Tuple2<>("Mary", 29),
                new Tuple2<>("James", 8));

        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(tuple);

        pairRDD.coalesce(1).saveAsTextFile("src/main/resources/paird");
    }


}
