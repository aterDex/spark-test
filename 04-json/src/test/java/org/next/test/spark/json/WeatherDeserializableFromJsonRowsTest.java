package org.next.test.spark.json;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.next.test.spark.json.JsonTest.INPUT_FILE_JSON;
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
        JavaRDD<Weather> weatherRdd = sc.textFile(INPUT_FILE_JSON)
                .mapPartitions(new WeatherDeserializableFromJsonRows());
        List<Weather> result = weatherRdd.take(10);
        printLogWithName(result, "Weather", null);
        assertEquals(10, result.size());
    }

    @Test
    void callWholeFile() {
        JavaPairRDD<LongWritable, Text> weatherRdd = sc.hadoopFile(INPUT_FILE_JSON, InputFormatWeatherJson.class, LongWritable.class, Text.class);
        List<Weather> result = weatherRdd
                .mapValues(Object::toString)
                .mapValues(new WeatherDeserializedFromJson())
                .values()
                .take(10);
        printLogWithName(result, "Weather", null);
        assertEquals(10, result.size());
    }
}