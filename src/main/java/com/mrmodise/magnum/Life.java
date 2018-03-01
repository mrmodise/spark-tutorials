package com.mrmodise.magnum;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.Tuple1;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class Life {
    // lifeName the name/id of the life
    private String lifeName;
    // keyprivate static final Stringue all key private static final Stringue attributes related to the life
    private Map<String,String> keyvalue;
    // policies all policies of the life
    private List<Policy> policies;

    /**
     * The regex patterns which will be used throughout the code
     * (?s) means newlines are ignored
     */
    private static final String CLEAN = "[0-9]{15},[0-9]{15},";
    private static final String POLICY = "(?s).*(POLICY) ([0-9]+).*";
    private static final String LIFE = "(?s).*(LIFE) ([0-9]+).*";
    private static final String KEY = "(?s).*(key=\")([0-9]+)\".*";
    private static final String STATEMENT = "(.+)=(.+)";
    private static final String MAGNUMLOG = "<MAGNUM.LOG>(.+?)</MAGNUM.LOG>";
    private static final String MAGNUMXML = "(?s)\\Q<?xml version='1.0' ?>\\E(.*?)\\Q</magnumlog>\\E";

    /**
     * Apply method to convert String, String to Life
     * @param wholeFiles full text files in one of the 2 possible formats
     * @return RDD of Life
     */
    public void apply(JavaPairRDD<String, String> wholeFiles){

        // Delete row and line number and SUBSTITUTE character
        JavaRDD<String> cleaned = wholeFiles.map(x -> (x._1.replaceAll(CLEAN, "")));

        System.out.println(cleaned.first());
        // Now extract the two formats out of every file (one file might contain two formats

        // Apply correct format
    }
}

class Policy {
    private String policyNumber;
    private String lifeName;
    private Map<String, String> keyvalue;
}
