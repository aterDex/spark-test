package org.next.test.spark.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import lombok.extern.slf4j.Slf4j;

import java.io.StringWriter;

@Slf4j
public final class JsonHelper {

    private JsonHelper() {
    }

    public static String extractedObjectAsString(JsonParser parser, JsonFactory factory) {
        if (!(parser.getCurrentToken() == JsonToken.START_OBJECT || parser.getCurrentToken() == JsonToken.START_ARRAY)) {
            throw new RuntimeException("Only START OBJECT or START ARRAY");
        }
        int counter = 0;
        try (StringWriter sw = new StringWriter();
             JsonGenerator generator = factory.createGenerator(sw)) {
            generator.writeStartObject();
            while (true) {
                JsonToken nt = parser.nextToken();
                switch (nt) {
                    case VALUE_NULL:
                        generator.writeNull();
                        break;
                    case VALUE_TRUE:
                        generator.writeBoolean(true);
                        break;
                    case VALUE_FALSE:
                        generator.writeBoolean(false);
                        break;
                    case VALUE_STRING:
                        generator.writeString(parser.getValueAsString());
                        break;
                    case VALUE_EMBEDDED_OBJECT:
                    case NOT_AVAILABLE:
                        break;
                    case VALUE_NUMBER_FLOAT:
                    case VALUE_NUMBER_INT:
                        generator.writeRawValue(parser.getText());
                        break;
                    case START_ARRAY:
                        generator.writeStartArray();
                        break;
                    case END_ARRAY:
                        generator.writeEndArray();
                        break;
                    case FIELD_NAME:
                        generator.writeFieldName(parser.getCurrentName());
                        break;
                    case END_OBJECT:
                        generator.writeEndObject();
                        if (counter == 0) {
                            generator.flush();
                            sw.flush();
                            return sw.toString();
                        }
                        counter--;
                        break;
                    case START_OBJECT:
                        counter++;
                        generator.writeStartObject();
                        break;
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
}
