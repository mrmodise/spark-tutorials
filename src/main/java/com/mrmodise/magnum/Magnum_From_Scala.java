package com.mrmodise.magnum;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.function.Function;

public class Magnum_From_Scala {

        private static final String CLEAN = "^[0-9]{15},[0-9]{15},";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Java Pair RDD")
                .set("spark.hadoop.validateOutputSpecs", "false")
                .setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> magnumRDD = sc.textFile("src/main/resources/logformat.log");

        JavaPairRDD<String, String> magnumPairRDD = magnumRDD
                .mapToPair(extractLog());

//        JavaPairRDD<Tuple2<String, String>, Long> rdd = magnumPairRDD.zipWithIndex();

        JavaPairRDD<String, String> jrp = magnumPairRDD
                .filter(keyValue -> !keyValue._1().equals(""));


//        JavaPairRDD<Tuple2<String, String>, Long> rddNoNulls = rdd.filter(keyValue -> !keyValue._1().equals(""));

        jrp.coalesce(1).saveAsTextFile("src/main/resources/magnum-pair-rdd");
    }

    private static PairFunction<String, String, String> extractLog() {
        return line -> new Tuple2<>(line.replaceAll(CLEAN,""), "");
    }

    private static PairFunction<String, String, String> removeComma() {
        return line -> new Tuple2<>(line.replaceAll(",",""),"");
    }
}
