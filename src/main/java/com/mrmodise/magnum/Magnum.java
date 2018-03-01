package com.mrmodise.magnum;

import com.google.common.base.Splitter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Magnum {

    final static Predicate<String> pattern = Pattern.compile("MAGNUM.LOG\\>(.+?)\\</MAGNUM.LOG").asPredicate();

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> results = spark.read()
                .option("header", "false")
                .csv("src/main/resources/logformat.log");

        List<String> dr = results
                .select("_c2")
                .as(Encoders.STRING())
                .collectAsList();

//        dr.stream().map(line -> line.split("MAGNUM.LOG\\>(.+?)\\</MAGNUM.LOG")).forEach(System.out::print);

        results.registerTempTable("magnum");


        spark.sql("select _c2 from magnum").show(100,false);

//        dr.stream().forEach(System.out::print);
    }
}
