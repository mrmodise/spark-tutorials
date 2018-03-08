package com.mrmodise.magnum;

import com.google.common.base.Splitter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import static com.mrmodise.Utils.POLICY;
import static com.mrmodise.Utils.STATEMENT;

public class MagnumLog {
    public static void main(String[] args) throws AnalysisException {
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
        dr.forEach(str -> magnumLog.append(str + " "));
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

        pairedFromRDD.foreach(data -> System.out.println(data._1() + " " + data._2()));

        /**      JavaPairRDD<String, Iterable<String>> groupedPairedRDD = pairedFromRDD.groupByKey();


         for (Map.Entry<String, Iterable<String>> magnumByLife: groupedPairedRDD.collectAsMap().entrySet()) {

         System.out.println(magnumByLife.getKey() + " ---> " + magnumByLife.getValue());
         }
         */
       /*
       // Dataset approach
       Dataset<Row> magnumData = spark
                .createDataset(pairedFromRDD.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF();
        long count1 = magnumData.select(magnumData.col("_1").).count();
        System.out.println("LIFE counts is " + count1);
        magnumData.printSchema();
        magnumData.show();*/
    }

    /**
     * @return
     */
    private static PairFunction<String, String, String> getProcessedData() {
        return (PairFunction<String, String, String>) MagnumLog::extractInformation;
    }

    /**
     * @param s
     * @return
     */
    private static Tuple2<String, String> extractInformation(String s) {

        StringBuilder cleanSentencesKey = new StringBuilder();
        StringBuilder cleanSentencesValue = new StringBuilder();

        String sentences = s
                .replaceAll("null", ""); // we don't need nulls

        // split on colon then retrieve a key value map from the split on the hash (#)
        // exclude whitespaces
        List<String> list = Splitter.onPattern("(:|b|\\[)").omitEmptyStrings()
                .trimResults()
                .splitToList(sentences);

        list.stream().filter(x -> x.contains("POLICY"))
                .map(MagnumLog::extractKeyValuesLog)
                .collect(Collectors.toList()).forEach(System.out::println);

//        System.out.println(cleanSentencesKey.toString());

       /* map.forEach((key, value) -> {
            cleanSentencesKey.append(key + " " + value);
            cleanSentencesValue.append(value + " ");
        });*/

        return new Tuple2<>("", "");
    }

    /**
     * Helper function to extract key value pairs in a block of text (multiple lines) for the log format
     * @param block
     * @return
     */
    private static Map<Integer, Map<String, String>> extractKeyValuesLog(String block) {
        // Find all STATEMENT patterns in the block
        Matcher matcher = Pattern.compile(STATEMENT).matcher(block);
        // Find all POLICY patterns in the block
        Matcher policies = Pattern.compile(POLICY).matcher(block);
        // Placeholder to load all statements that match the pattern
        StringBuilder matches = new StringBuilder();
        // Placeholder to load all statements that match the policy pattern
        StringBuilder policyMatches = new StringBuilder();
        // Extract matches into the placeholders
        while (matcher.find()) matches.append(matcher.group(0) + " ");
        while (policies.find()) policyMatches.append(policies.group(0) + "_");

        // Put the policy numbers in a map
        Map<String, String> mapNumbers = Splitter.on("_")
                .omitEmptyStrings().trimResults()
                .withKeyValueSeparator(" ")
                .split(policyMatches.toString());

        // Put matched statements in a key value map
        Map<String, String> map = Splitter.on(" ")
                .omitEmptyStrings()
                .trimResults()
                .withKeyValueSeparator("=")
                .split(matches.toString());
        // Build policy-key-value relationship
        Map<Integer, Map<String, String>> finalMap = new LinkedHashMap<>();
        // Retrieve the policy number only
        finalMap.put(Integer.parseInt(mapNumbers.get("POLICY")), map);
        return finalMap;
    }

    /**
     * Counts the number of pattern matches on a particular string
     *
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
