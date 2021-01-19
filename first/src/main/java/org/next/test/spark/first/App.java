package org.next.test.spark.first;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;

public class App {

    public static final String INPUT_FILE = "data/first/example.txt";
    public static final String OUTPUT_DIR = "data/first/example_result";

    public static void main(String[] args) throws Exception {
        removeOldResult();
        sparkStart(initSpark());
    }

    private static void sparkStart(JavaSparkContext sc) {
        JavaRDD<String> input = sc.textFile(INPUT_FILE);
        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts = words
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
        counts.saveAsTextFile(OUTPUT_DIR);
    }

    private static JavaSparkContext initSpark() {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

    private static void removeOldResult() throws IOException {
        Files.walkFileTree(Paths.get(OUTPUT_DIR),
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult postVisitDirectory(
                            Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(
                            Path file, BasicFileAttributes attrs)
                            throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }
                });
    }
}
