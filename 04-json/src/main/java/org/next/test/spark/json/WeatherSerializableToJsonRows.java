package org.next.test.spark.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.AbstractIterator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.next.test.spark.utils.JacksonUtils;

import java.util.Iterator;

@Slf4j
public class WeatherSerializableToJsonRows implements FlatMapFunction<Iterator<Weather>, String> {

    @Override
    public Iterator<String> call(Iterator<Weather> weathers) throws Exception {
        ObjectMapper mapper = JacksonUtils.initObjectMapperWithJava8Modules();
        return new AbstractIterator<String>() {
            @Override
            protected String computeNext() {
                if (weathers.hasNext()) {
                    try {
                        Weather weather = weathers.next();
                        return mapper.writeValueAsString(weather);
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
                return endOfData();
            }
        };
    }
}
