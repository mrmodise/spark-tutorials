package com.mrmodise.magnum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class MagnumXml {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        // XML PROCESSING
        Dataset<Row> r = spark.read()
                .option("header", "false")
                .csv("src/main/resources/xmlformat.xml");

        Dataset<Row> xmlData = spark.read()
                .format("com.databricks.spark.xml")
                .option("rootTag", "magnumlog")
                .option("rowTag", "section")
                .load("src/main/resources/xmlformat.xml");

        Dataset<Row> policies = spark.read()
                .format("com.databricks.spark.xml")
                .option("rootTag", "policy")
                .option("rowTag", "definition")
                .load("src/main/resources/xmlformat.xml");

        policies.show();
    }
}
