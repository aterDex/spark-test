package org.next.test.spark.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.AbstractIterator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.next.test.spark.utils.JacksonUtils;

import java.util.Iterator;

@Slf4j
public class WeatherDeserializableFromJsonRows implements FlatMapFunction<Iterator<String>, Weather> {

    @Override
    public Iterator<Weather> call(Iterator<String> lines) {
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
}
