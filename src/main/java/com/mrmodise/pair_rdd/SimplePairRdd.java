package com.mrmodise.pair_rdd;

import scala.Tuple2;

public class SimplePairRdd {
    public static void main(String[] args) {
        Tuple2<Integer, String> tuple2 = new Tuple2<>(12, "value");
        Integer key = tuple2._1();
        String value = tuple2._2();
        System.out.printf("Key is %d and Value is %s", key, value);
    }
}
