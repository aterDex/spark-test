package org.next.test.spark.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.Collection;
import java.util.function.Function;

@Slf4j
public final class SparkUtils {

    private SparkUtils(){}

    public static JavaSparkContext initDefaultSpark() {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Default spark context");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

    public static <T> void printLogWithName(Collection<T> collect, String name, Function<T, String> converter) {
        final Function<T, String> safeConverter = converter != null ? converter : String::valueOf;
        Marker marker = MarkerFactory.getMarker(name);
        log.info(marker, "===================================================");
        collect.forEach(x -> log.info(marker, safeConverter.apply(x)));
        log.info(marker, "===================================================");
    }
}
