/*
 * Bell Canada, Network Big Data Team
 * Dev: Aws Alsamarrie
 */

package spark-testing;

/**
 * Created by Aws Alsamarrie on 05-Oct-2016.
 * credits to: ANTOINE AMEND
 * https://hadoopi.wordpress.com/category/inputformat/
 *
 * changes done by Aws Alsamarrie:
 * > support of compressed source files (the source data was gzip). TODO - support splittable compressed formats like bzip2
 * > addition of line delimiter specification
 * > begin and end line regex: now it picks up lines between 2 regex record delimiters
 */

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import com.google.common.base.Charsets;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;


public class PatternRecordReader2 extends RecordReader<LongWritable, Text> {

    private LineReader in;
    private long start;
    private long pos;
    private long end;
    private LongWritable key = new LongWritable();
    private Text value = new Text();
    //private static final Text EOL = new Text("\n");
    private Pattern lineBeginPattern;
    private Pattern lineEndPattern;
    private String lineBeginRegex;
    private String lineEndRegex;
    private String delimiter;
    private int maxLengthRecord;
    private Text lEnd;
    // private Text lastDelimValue = new Text();

    private static final Log LOG = LogFactory.getLog(RecordReader.class);

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        // Retrieve configuration value
        Configuration job = context.getConfiguration();

        this.lineBeginRegex = job.get("record.line.begin.regex");
        this.lineEndRegex = job.get("record.line.end.regex");

        //this.maxLengthRecord = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.maxLengthRecord = job.getInt("mapreduce.input.linerecordreader.line.maxlength", Integer.MAX_VALUE);

        this.delimiter = job.get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes;
        if (null != delimiter) {
            lEnd = new Text(delimiter);
            recordDelimiterBytes = lEnd.toString().getBytes(Charsets.UTF_8);
        } else{
            lEnd = new Text("\n");
            recordDelimiterBytes = null;
        }

        // Compile pattern only once per InputSplit
        lineBeginPattern = Pattern.compile(lineBeginRegex);
        lineEndPattern = Pattern.compile(lineEndRegex);

        // Retrieve FileSplit details
        FileSplit fileSplit = (FileSplit) split;
        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        final Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(job);

        // Open FileSplit FSDataInputStream
        FSDataInputStream fileIn = fs.open(fileSplit.getPath());

        // assumption: files are gzipped - which is not a splittable codec
        CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
        if (codec != null) {
            LOG.info("Reading compressed file: " + file);
            in = new LineReader(codec.createInputStream(fileIn), job, recordDelimiterBytes);
            end = Long.MAX_VALUE;
        } else {
            LOG.info("Reading uncompressed file: " + file);
            // Skip first record if Split does not start at byte 0 (first line of file)
            boolean skipFirstLine = false;
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }

            // Read FileSplit content
            in = new LineReader(fileIn, job, recordDelimiterBytes);
            if (skipFirstLine) {
                LOG.info("Need to skip first line of Split");
                Text dummy = new Text();
                start += readNext(dummy, 0,
                        (int) Math.min((long) Integer.MAX_VALUE, end - start));
            }

            this.pos = start;
        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        key.set(pos);
        int newSize = 0;

        // Get only the records for which the first byte
        // is located before end of current Split
        while (pos < end) {

            // Read new record and store content into value
            // value is a mutable parameter
            newSize = readNext(value, maxLengthRecord, Math.max(
                    (int) Math.min(Integer.MAX_VALUE, end - pos),
                    maxLengthRecord));

            pos += newSize;
            if (newSize == 0) {
                break;
            }
            if (newSize < maxLengthRecord) {
                break;
            }

            LOG.error("Skipped radius of size " + newSize + " at pos "
                    + (pos - newSize));
        }

        // No bytes to read (end of split)
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return start == end ? 0.0f : Math.min(1.0f, (pos - start)
                / (float) (end - start));
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    private int readNext(Text text, int maxLineLength, int maxBytesToConsume)
            throws IOException {

        int offset = 0;
        text.clear();
        Text tmp = new Text();

        // set linesOfInterest flag to false
        boolean linesOfInterest = false; // flag to read the lines
        boolean firstLine = false;  //flag to indicate it's the first, so that we dont add new line at the begining

        for (int i = 0; i < maxBytesToConsume; i++) {

            int offsetTmp = in.readLine(tmp, maxLineLength, maxBytesToConsume);
            offset += offsetTmp;
            Matcher mBegin = lineBeginPattern.matcher(tmp.toString());
            Matcher mEnd = lineEndPattern.matcher(tmp.toString());

            // End of File
            if (offsetTmp == 0) {
                break;
            }

            if (mBegin.matches() && !linesOfInterest) {
                linesOfInterest = true;
                firstLine = true;
            }

            if (linesOfInterest) {
                if (!firstLine) {
                    text.append(lEnd.getBytes(), 0, lEnd.getLength());
                    text.append(tmp.getBytes(), 0, tmp.getLength());
                } else {
                    text.append(tmp.getBytes(), 0, tmp.getLength());
                    firstLine = false;  //next lines (iterations) will be next lines
                }
            }

            if (mEnd.matches() && linesOfInterest){
                // the text will be detected from previous if, i.e. you will also get the line it found mEnd
                break;
            }

        }

        return offset;
    }

}

