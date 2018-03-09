package com.mrmodise.rdd_dataset;

import com.mrmodise.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.commons.lang.math.NumberUtils.toInt;

public class RddDatasetConversion {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf()
                .setAppName("StackOverFlowSurvey")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession session = SparkSession.builder().appName("StackOverFlowSurvey")
                .master("local[1]").getOrCreate();

        JavaRDD<String> lines = sc.textFile("test-data/2016-stack-overflow-survey-responses.csv");

        JavaRDD<Response> responseRDD = lines
                .filter(line -> !line.split(Utils.COMMA_DELIMITER, -1)[2].equals("country"))
                .map(line -> {
                    String[] splits = line.split(Utils.COMMA_DELIMITER, -1);
                   return new Response(splits[2], toInt(splits[6]), splits[9], toInt(splits[14]));
                });

        Dataset<Response> responseDataset = session.createDataset(responseRDD.rdd(), Encoders.bean(Response.class));

        System.out.println("=== Print out Schema ===");
        responseDataset.printSchema();

        System.out.println("=== Print 20 records of responses table ===");
        responseDataset.show();

        JavaRDD<Response> responseJavaRDD = responseDataset.toJavaRDD();

        for (Response response: responseJavaRDD.collect()) {
            System.out.println(response);
        }
    }
}
