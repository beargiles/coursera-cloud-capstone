package com.coyotesong.coursera.cloud.hadoop.mapreduce.lib.output;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.coyotesong.coursera.cloud.hadoop.io.AirlineFlightDelaysWritable;
import com.coyotesong.coursera.cloud.hadoop.io.AirportsAndAirlineWritable;

public class AirportsAndAirlineFlightDelaysOutputFormat extends TextOutputFormat<AirportsAndAirlineWritable, AirlineFlightDelaysWritable> {
    private static final String EXTENSION = "";
    @Override
    public RecordWriter<AirportsAndAirlineWritable, AirlineFlightDelaysWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Path output = getDefaultWorkFile(context, EXTENSION);
        return new AirportsAndAirlineFlightDelaysRecordWriter(context, output);
    }

    private static class AirportsAndAirlineFlightDelaysRecordWriter extends RecordWriter<AirportsAndAirlineWritable, AirlineFlightDelaysWritable> {
        private PrintWriter pw;
        
        public AirportsAndAirlineFlightDelaysRecordWriter(TaskAttemptContext context, Path output) throws IOException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = output.getFileSystem(conf);
            FSDataOutputStream fsOutputStream = fs.create(output);
            pw = new PrintWriter(new OutputStreamWriter(fsOutputStream));
        }

        @Override
        public void write(AirportsAndAirlineWritable key, AirlineFlightDelaysWritable value) throws IOException, InterruptedException {
            pw.printf("%d,%d,%d\t%d,%d,%d,%d\n", key.getOriginId(), key.getDestinationId(), key.getAirlineId(),
                    value.getNumFlights(), value.getDelay(),
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
