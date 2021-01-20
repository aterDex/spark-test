package org.next.test.spark.json;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.next.test.spark.json.JsonTest.INPUT_FILE_JSON;
import static org.next.test.spark.utils.SparkUtils.initDefaultSpark;
import static org.next.test.spark.utils.SparkUtils.printLogWithName;

class WeatherSerializableToJsonRowsTest {

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

        JavaRDD<Weather> weatherRddFiltered = weatherRdd
                .filter(x -> x.getDate() != null)
                .filter(x -> x.getDate().isAfter(LocalDate.of(2020, 11, 01).minusDays(1))
                        && x.getDate().isBefore(LocalDate.of(2020, 12, 01))
                );

        JavaRDD<String> jsonRows = weatherRddFiltered.mapPartitions(new WeatherSerializableToJsonRows());
        List<String> jsonRowsList = jsonRows.collect();

        printLogWithName(jsonRowsList, "WeatherJson", null);
        assertEquals(30, jsonRowsList.size());
    }
}