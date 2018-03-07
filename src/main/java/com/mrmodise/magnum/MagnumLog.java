package com.mrmodise.magnum;

import com.google.common.base.Splitter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
                .csv("test-data/MAG_TRACE_NOV07.CSV"); // replace with

        // Retrieve last column with required data
        List<String> dr = results
                .select("_c2")
                .as(Encoders.STRING())
                .collectAsList();

        // Extract records from the magnum tags
        StringBuilder magnumLog = new StringBuilder();
        Pattern p = Pattern.compile("<MAGNUM.LOG>(.+?)</MAGNUM.LOG>");
        dr.forEach(str -> magnumLog.append(str + ";"));
        Matcher m = p.matcher(magnumLog.toString());
        // Count any matches to the regex above
        int finalCount = countMatches(p, magnumLog.toString());
        int count = 0;
        // Add up all found matches to the list
        List<String> list = new ArrayList<>();
        while (true) {
            if (!(m.find() && (count <= finalCount))) break;
            list.add(count, m.group(1));
            count++;
        }
        // Create spark context from Spark session. Needed to convert list to JavaPairRDD
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        // Convert list to JavaRDD
        JavaRDD<String> fromListRDD = sc.parallelize(list);
        // Extract key-value pairs from the data
        JavaPairRDD<String, String> pairedFromRDD = fromListRDD
                .mapToPair(getProcessedData());

        pairedFromRDD.foreach(data -> {
            int total = 0;
            total = total + extractLifeCount(data._1());
            System.out.println("TOTAL " + total);
            System.out.println(data._1() + " " + data._2());
        });
    }

    /**
     *
     * @return
     */
    private static PairFunction<String, String, String> getProcessedData() {
        return (PairFunction<String, String, String>) MagnumLog::extractInformation;
    }

    /**
     *
     * @param s
     * @return
     */
    private static Tuple2<String, String> extractInformation(String s) {

        StringBuilder cleanSentencesKey = new StringBuilder();
        StringBuilder cleanSentencesValue = new StringBuilder();

        String sentences = s
                .replaceAll("null", "") // we don't need nulls
                .replaceAll(";;", "") // we only need a single occurence not double
                .replaceAll("(\\d+)\\:(\\d+)", "00"); // we do not need this for now

        // split on colon then retrieve a key value map from the split on the hash (#)
        // exclude whitespaces
        Map<String, String> map = Splitter.on(":")
                .omitEmptyStrings()
                .trimResults()
                .withKeyValueSeparator("#")
                .split(sentences);

        map.forEach((key, value) -> {
            cleanSentencesKey.append(key + "" + value);
        });
        return new Tuple2<>(cleanSentencesKey.toString(), "");
    }

    /**
     *
     * @param key
     * @return
     */
    private static int extractLifeCount(String key) {
        Pattern pattern = Pattern.compile("\\b(LIFE) ([0-9]+)\\b");
        return countMatches(pattern, key);
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
