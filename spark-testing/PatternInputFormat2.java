/*
 * Bell Canada, Network Big Data Team
 * Dev: Aws Alsamarrie
 */

package spark-testing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import java.io.IOException;

/**
 * Created by Aws Alsamarrie on 05-Oct-2016..
 *
 */

// path of research: TextInputFormat.java , LineRecordReader.java , LineReader.java

public class PatternInputFormat2 extends TextInputFormat {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new PatternRecordReader2();
    }
}


