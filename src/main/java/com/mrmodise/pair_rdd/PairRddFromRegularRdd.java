package com.mrmodise.pair_rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PairRddFromRegularRdd {
    public static void main(String[] args) {
        // Create a Spark session on local cluster
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
                .config("spark.master", "local[4]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<String> inputStrings = Arrays.asList("Lily 23", "Jack 29", "Mary 29", "James 8");

        JavaRDD<String> regularRDDs = sc.parallelize(inputStrings);

        JavaPairRDD<String, Integer> pairRDD = regularRDDs.mapToPair(getNameAndAgePair());

        pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd");
    }

    private static PairFunction<String, String, Integer> getNameAndAgePair() {
        return s -> new Tuple2<>(
                s.split(" ")[0],
                Integer.valueOf(s.split(" ")[1])
        );
    }

    /**
     * public interface PairFunction<T, K, V> extends Serializable {
     *      Tuple2<K = tuple key, V = tuple value> call(T t) throws Exception;
     *  }
     */
}
