package org.next.test.spark.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.AbstractIterator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.next.test.spark.utils.JacksonUtils;

import java.util.Iterator;

@Slf4j
public class WeatherSource {

    private final String source;

    public WeatherSource(String source) {
        this.source = source;
    }

    public static Iterator<Weather> convertToWeather(Iterator<String> lines) throws Exception {
        ObjectMapper mapper = JacksonUtils.initObjectMapperWithJava8Modules();
        return new AbstractIterator<Weather>() {
            @Override
            protected Weather computeNext() {
                if (lines.hasNext()) {
                    try {
                        String line = lines.next();
                        return mapper.readValue(line, Weather.class);
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
                return endOfData();
            }
        };
    }

    public JavaRDD<Weather> createRdd(JavaSparkContext sc) {
        JavaRDD<String> input = sc.textFile(source);
        return input.mapPartitions(WeatherSource::convertToWeather);
    }
}
