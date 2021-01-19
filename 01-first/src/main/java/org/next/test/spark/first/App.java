package org.next.test.spark.first;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

@Slf4j
public class App {

    public static final String INPUT_FILE = "data/first/example.txt";

    public static void main(String[] args) throws Exception {
        printLog(calc(initSpark()));
    }

    private static void printLog(Map<String, Integer> calc) {
        log.info("===================================================");
        for (Map.Entry<String, Integer> stringIntegerEntry : calc.entrySet()) {
            log.info("'{}' {}", stringIntegerEntry.getKey(), stringIntegerEntry.getValue());
        }
        log.info("===================================================");
    }

    private static Map<String, Integer> calc(JavaSparkContext sc) {
        JavaRDD<String> input = sc.textFile(INPUT_FILE);
        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts = words
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
        return counts.collectAsMap();
    }

    private static JavaSparkContext initSpark() {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }
}
