package org.next.test.spark.json;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.next.test.spark.json.JsonTest.INPUT_FILE;
import static org.next.test.spark.utils.SparkUtils.initDefaultSpark;
import static org.next.test.spark.utils.SparkUtils.printLogWithName;

class WeatherDeserializableFromJsonRowsTest {

    private static JavaSparkContext sc;

    @BeforeAll
    static void init() {
        sc = initDefaultSpark();
    }

    @AfterAll
    static void close() {
        sc.close();
    }

    @Test
    void call() {
        JavaRDD<Weather> weatherRdd = sc.textFile(INPUT_FILE)
                .mapPartitions(new WeatherDeserializableFromJsonRows());
        List<Weather> result = weatherRdd.take(10);
        printLogWithName(result, "Weather", null);
        assertEquals(10, result.size());
    }
}