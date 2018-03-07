package com.mrmodise.magnum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MagnumLog {
    public static void main(String[] args) {
        // Create a Spark session on local cluster
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
                .config("spark.master", "local[4]")
                .getOrCreate();

        // Read log file into a data-set row
        Dataset<Row> results = spark.read().option("header", "false")
                .csv("magnum/MAG_TRACE_NOV07.CSV"); // replace with

        // Retrieve last column with required data
        List<String> dr = results
                .select("_c2")
                .as(Encoders.STRING())
                .collectAsList();

        // Preparing
        StringBuilder magnumLog = new StringBuilder();
        Pattern p = Pattern.compile("<MAGNUM.LOG>(.+?)</MAGNUM.LOG>");
        dr.forEach(str -> {
            magnumLog.append(str + ";");
        });
        Matcher m = p.matcher(magnumLog.toString());
        // Count any matches to the regex above
        int finalCount = countMatches(p, magnumLog.toString());
        int count = 0;
        List<String> list = new ArrayList<>();

        // Add up all matches to the list
        while (true) {
            if (!(m.find() && (count <= finalCount))) break;
            list.add(count, m.group(1));
            count++;
        }

        list.stream().forEach(System.out::println);

//        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

//        JavaPairRDD<Integer, String> javaPairRDD = sc.parallelizePairs(list);

//        collect.stream().filter(line -> line.contains("POLICY")).forEach(System.out::println);


       /* Dataset<Row> dataset = spark.createDataset(collect, Encoders.STRING()).toDF();
        dataset.printSchema();
        dataset.show();

        Dataset<Row> row = dataset.selectExpr("split(value, '\\b\\w+\\s\\d{1}\\b')[0] as life", "split(value, '\\b\\w+\\s\\d{1}\\b')[1] policy");
        row.printSchema();
        row.show();*/

    }

    /**
     * Counts the number of pattern matches on a particular string
     * @param pattern
     * @param str
     * @return
     */
    static int countMatches(Pattern pattern, String str) {
        int matches = 0;
        Matcher matcher = pattern.matcher(str);
        while (matcher.find())
            matches++;
        return matches;
    }
}
