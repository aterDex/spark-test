package org.next.test.spark.aggregate;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

@Slf4j
public class App {

    public static final String INPUT_FILE = "data/first/example.txt";

    public static void main(String[] args) throws Exception {
        printLog(calc(initSpark()));
    }

    private static void printLog(AvgCount avg) {
        log.info("===================================================");
        log.info("avg: {}", avg.avg());
        log.info("===================================================");
    }

    private static AvgCount calc(JavaSparkContext sc) {
        JavaRDD<String> input = sc.textFile(INPUT_FILE);
        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        return words
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y)
                .map(Tuple2::_2)
                .aggregate(new AvgCount(0, 0),
                        (ac, v) -> ac.plusTotal(v).plusCount(1),
                        AvgCount::union);
    }

    private static JavaSparkContext initSpark() {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }
}
