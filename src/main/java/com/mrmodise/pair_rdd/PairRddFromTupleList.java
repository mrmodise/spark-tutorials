package com.mrmodise.pair_rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PairRddFromTupleList {
    public static void main(String[] args) {
        // Create a Spark session on local cluster
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
                .config("spark.master", "local[4]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<Tuple2<String, Integer>> tuple = Arrays.asList(
                new Tuple2<>("Lily", 23),
                new Tuple2<>("Jack", 29),
                new Tuple2<>("Mary", 28),
                new Tuple2<>("James", 3),
                new Tuple2<>("Botlhale", 5));

        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(tuple);

        pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_tuple_list");
    }
}
