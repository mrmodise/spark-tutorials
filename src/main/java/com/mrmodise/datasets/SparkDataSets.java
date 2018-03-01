package com.mrmodise.datasets;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDataSets {
    public static void main(String[] args) throws AnalysisException {
        /**
         * Create a Spark session
         * provides builtin support for Hive features including the ability to write queries using HiveQL
         * access to Hive UDFs, and the ability to read data from Hive tables.
         */
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        // create a data set row from json
        Dataset<Row> df = spark.read().json("src/main/resources/people.json");

        // display data set
        df.show();

        // display schema properties
        df.printSchema();

        // select only the name
        df.select("name").show();

        // select name and age, increment ages by 1
        df.select(df.col("name"), df.col("age").plus(1)).show();

        // Select people older than 21
        df.filter(df.col("age").gt(21)).show();

        // Count people by age
        df.groupBy("age").count().show();

        // Registering data set as a global temporary view
        df.createGlobalTempView("people");

        spark.sql("select * from global_temp.people").show();

    }
}
