package org.next.test.spark.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.Function;
import org.next.test.spark.utils.JacksonUtils;

@Slf4j
public class WeatherDeserializedFromJson implements Function<String, Weather> {

    private ObjectMapper mapper;

    @Override
    public Weather call(String line) throws Exception {
        if (mapper == null) {
            mapper = JacksonUtils.initObjectMapperWithJava8Modules();
        }
        try {
            return mapper.readValue(line, Weather.class);
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
}
