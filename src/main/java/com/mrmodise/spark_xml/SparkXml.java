package com.mrmodise.spark_xml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkXml {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        SQLContext sc = new SQLContext(spark);

        Dataset df = sc.read()
                .format("com.databricks.spark.xml")
//                .option("")
                .load("src/main/resources/xmlformat.xml");

        df.show();


    }
}
