package com.coyotesong.coursera.cloud.hadoop.mapreduce.lib.output;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.coyotesong.coursera.cloud.hadoop.io.AirlineFlightDelaysWritable;

public class AirlineFlightDelaysOutputFormat extends TextOutputFormat<IntWritable, AirlineFlightDelaysWritable> {
    private static final String EXTENSION = "";
    @Override
    public RecordWriter<IntWritable, AirlineFlightDelaysWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Path output = getDefaultWorkFile(context, EXTENSION);
        return new AirlineFlightDelaysRecordWriter(context, output);
    }

    private static class AirlineFlightDelaysRecordWriter extends RecordWriter<IntWritable, AirlineFlightDelaysWritable> {
        private PrintWriter pw;
        
        public AirlineFlightDelaysRecordWriter(TaskAttemptContext context, Path output) throws IOException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = output.getFileSystem(conf);
            FSDataOutputStream fsOutputStream = fs.create(output);
            pw = new PrintWriter(new OutputStreamWriter(fsOutputStream));
        }

        @Override
        public void write(IntWritable key, AirlineFlightDelaysWritable value) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            pw.printf("%d\t%d,%d,%d,%d\n", value.getAirlineId(), value.getNumFlights(), value.getDelay(),
                    value.getDelaySquared(), value.getMaxDelay());
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (pw != null) {
                pw.flush();
                pw.close();
                pw = null;
            }
        }
    }
}
