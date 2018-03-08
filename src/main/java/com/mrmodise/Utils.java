package com.mrmodise;

public class Utils {
    private Utils(){

    }

    public static final String STATEMENT = "\\b(\\w+=\\w+\\s\\d|\\w+=\\w+)\\b";
    public static final String POLICY = "(POLICY) ([0-9]+)";
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
}
