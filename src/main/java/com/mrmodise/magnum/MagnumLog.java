package com.mrmodise.magnum;

import com.google.common.base.Splitter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class MagnumLog {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        // LOG PROCESSING
        Dataset<Row> results = spark.read()
                .option("header", "false")
                .csv("src/main/resources/logformat.log");


        List<String> dr = results
                .select("_c2")
                .as(Encoders.STRING())
                .collectAsList();

        for (String str: dr) {
            System.out.println(str);
        }

//        spark.sparkContext().parallelize(dr);

//        results.registerTempTable("magnum");

//        spark.sql("select _c2 from magnum").show(100,false);
    }
}
