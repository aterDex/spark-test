package org.next.test.spark.transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class App {

    public static void main(String[] args) throws Exception {
        printLog(calc(initSpark()));
    }

    private static void printLog(List<Integer> calc) {
        log.info("===================================================");
        for (Integer i : calc) {
            log.info("{}", i);
        }
        log.info("===================================================");
    }

    private static List<Integer> calc(JavaSparkContext sc) {
        JavaRDD<Integer> base = sc.parallelize(Stream.iterate(0, x -> x + 1).limit(100).collect(Collectors.toList()));

        JavaRDD<Integer> mod10 = base
                .filter(x -> x % 10 == 0)
                .filter(x -> x != 0)
                .map(x -> x * 10000);

        JavaRDD<Integer> multi10 = base
                .map(x -> x * 10);

        JavaRDD<Integer> un = multi10
                .union(mod10);

        return un.collect();
    }

    private static JavaSparkContext initSpark() {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("transform");
        return new JavaSparkContext(conf);
    }
}
