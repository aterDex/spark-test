package org.next.test.spark.json;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.time.LocalDate;

public class InputFormatWeatherJson extends FileInputFormat<LongWritable, Text> {
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return new RecordReaderWeatherJson(job, (FileSplit) split);
    }
}
