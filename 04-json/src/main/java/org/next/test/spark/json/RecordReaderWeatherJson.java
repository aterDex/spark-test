package org.next.test.spark.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStream;

@Slf4j
public class RecordReaderWeatherJson implements RecordReader<LongWritable, Text> {

    private final FSDataInputStream fileIn;
    private final JsonParser jParser;
    private final JsonFactory jFactory;
    private boolean ret = true;
    private long counter = 0;

    public RecordReaderWeatherJson(Configuration job, FileSplit split) throws IOException {
        final Path file = split.getPath();
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        jFactory = new JsonFactory();
        jParser = jFactory.createParser((InputStream) fileIn);
    }

    @Override
    public synchronized boolean next(LongWritable key, Text value) throws IOException {
        try {
            while (true) {
                JsonToken nt = jParser.nextToken();
                if (nt == JsonToken.START_OBJECT) {
                    value.set(JsonHelper.extractedObjectAsString(jParser, jFactory));
                    key.set(++counter);
                    return true;
                }
                if (nt == JsonToken.END_ARRAY) {
                    return false;
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        try {
            jParser.close();
        } catch (Exception e) {
            log.error("", e);
        }
        fileIn.close();
    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }
}
