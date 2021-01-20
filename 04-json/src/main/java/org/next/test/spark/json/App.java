package org.next.test.spark.json;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import static org.next.test.spark.utils.SparkUtils.initDefaultSpark;
import static org.next.test.spark.utils.SparkUtils.printLogWithName;

@Slf4j
public class App {

    public static final String INPUT_FILE = "data/json/rdu-weather-history.txt";

    public static void main(String[] args) throws Exception {
        WeatherSource weatherSource = new WeatherSource(INPUT_FILE);
        JavaSparkContext sc = initDefaultSpark();
        JavaRDD<Weather> weatherRdd = weatherSource.createRdd(sc);

        printLogWithName(weatherRdd.take(10), "Weather", null);
    }
}
