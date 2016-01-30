package com.coyotesong.coursera.cloud.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.coyotesong.coursera.cloud.hadoop.io.AirlineFlightDelaysWritable;
import com.coyotesong.coursera.cloud.hadoop.mapreduce.lib.output.AirlineFlightDelaysOutputFormat;
import com.coyotesong.coursera.cloud.util.CSVParser;
import com.coyotesong.coursera.cloud.util.LookupUtil;

/**
 * Hadoop driver that identifies the airlines with the best on-time arrival
 * performance.
 * 
 * "Arrival performance" can be measured in many different ways. Compare airline
 * A where you will always be 6 minutes late with airline B where you will be
 * on-time 90% of the time but an hour late for the rest. Is one better than the
 * other? The average delay is the same.
 * 
 * I have decided to go with the 95th-percentile of the arrival delay as my
 * measure of arrival performance. The final 1-in-20 flights may be delayed due
 * to exceptional circumstances where a delay is preferable to flying in bad
 * weather, flying with minor mechanical concerns, etc.
 * 
 * It also doesn't make sense to compare an airline with one daily flight
 * between small airports with the major carriers. Therefore I limit the
 * statistics to the top 25 carriers as measured by number of flights.
 * 
 * (I think the usual metric is passenger miles and I could estimate that using
 * the recorded number of miles and type of aircraft but for now I'll assume
 * that all flights have the same number of passengers.)
 *
 * @author bgiles
 */
public class CarrierOnTimePerformanceDriver extends Configured implements Tool {

    /**
     * Set up first job - it reads input files and creates a file containing the
     * airline ID and basic arrival delay statistics.
     * 
     * @param input
     * @param output
     * @return
     */
    public Job setupFirstJob(Path input, Path output) throws IOException {
        final Job job = Job.getInstance(this.getConf(), "Gather arrival statistics");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(AirlineFlightDelaysWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(AirlineFlightDelaysWritable.class);

        job.setMapperClass(GatherArrivalDelayMap.class);
        job.setReducerClass(GatherArrivalDelayReduce.class);
        job.setCombinerClass(GatherArrivalDelayReduce.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setOutputFormatClass(AirlineFlightDelaysOutputFormat.class);

        job.setJarByClass(CarrierOnTimePerformanceDriver.class);

        return job;
    }

    /**
     * Set up second job - it reads file containing airline ID and arrival
     * statistics and performs the final comparisons.
     */
    public Job setupSecondJob(Path input, Path output) throws IOException {
        Job job = Job.getInstance(this.getConf(), "Top Airlines");
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(AirlineFlightDelaysWritable.class);

        job.setMapperClass(CompareArrivalDelayMap.class);
        job.setReducerClass(CompareArrivalDelayReduce.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(CarrierOnTimePerformanceDriver.class);

        return job;
    }

    /**
     * Run task. Arguments are INPUT DIR, OUTPUT DIR, TEMP DIR
     *
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        final Job jobA = setupFirstJob(new Path(args[0]), new Path(args[2]));
        boolean success = jobA.waitForCompletion(true);
        if (!success) {
            return 0;
        }

        final Job jobB = setupSecondJob(new Path(args[2]), new Path(args[1]));
        return jobB.waitForCompletion(true) ? 1 : 0;
    }

    /**
     * The mapper reads one line of the CSV file and produces a (airline, delay)
     * entry. The mapper output is designed to allow the use of a combiner.
     */
    public static class GatherArrivalDelayMap
            extends Mapper<LongWritable, Text, IntWritable, AirlineFlightDelaysWritable> {
        private int airlineIdx = 5;
        private int arrDelayIdx = 17;
        private int cancelledIdx = 18;
        private int divertedIdx = 19;

        @Override
        protected void setup(Mapper<LongWritable, Text, IntWritable, AirlineFlightDelaysWritable>.Context context) {
            // TODO: retrieve indexes
        }

        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, AirlineFlightDelaysWritable>.Context context)
                        throws IOException, InterruptedException {
            final List<String> values = CSVParser.parse(value.toString());

            // do not consider cancelled or diverted flights.
            final boolean cancelled = !"0.00".equals(values.get(cancelledIdx));
            final boolean diverted = !"0.00".equals(values.get(divertedIdx));
            if (cancelled || diverted) {
                return;
            }

            // get airlineID and arrival delay.
            final String airlineIdStr = values.get(airlineIdx);
            String arrDelayStr = values.get(arrDelayIdx);
            final int idx = arrDelayStr.indexOf('.');
            if (idx > 0) {
                arrDelayStr = arrDelayStr.substring(0, idx);
            }
            if (airlineIdStr.matches("[0-9]+") && arrDelayStr.matches("-?[0-9]+")) {
                final int airlineId = Integer.parseInt(airlineIdStr);
                final int delay = Integer.parseInt(arrDelayStr);
                context.write(new IntWritable(airlineId), new AirlineFlightDelaysWritable(airlineId, delay));
            }
        }
    }

    /**
     * The reducer adds up the delays for each airline
     */
    public static class GatherArrivalDelayReduce
            extends Reducer<IntWritable, AirlineFlightDelaysWritable, IntWritable, AirlineFlightDelaysWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<AirlineFlightDelaysWritable> values,
                Reducer<IntWritable, AirlineFlightDelaysWritable, IntWritable, AirlineFlightDelaysWritable>.Context context)
                        throws IOException, InterruptedException {
            final AirlineFlightDelaysWritable w = new AirlineFlightDelaysWritable(key.get());
            for (AirlineFlightDelaysWritable value : values) {
                w.add(value);
            }
            context.write(key, w);
        }
    }

    /**
     * This mapper loads the airline id and on-time arrival delay information
     */
    public static class CompareArrivalDelayMap extends Mapper<Text, Text, IntWritable, AirlineFlightDelaysWritable> {

        @Override
        protected void map(Text key, Text value,
                Mapper<Text, Text, IntWritable, AirlineFlightDelaysWritable>.Context context)
                        throws IOException, InterruptedException {
            final Integer airlineId = Integer.parseInt(key.toString());
            final String[] values = value.toString().split(",");
            final int[] ints = new int[4];
            for (int i = 0; i < ints.length; i++) {
                if (values[i].matches("-?[0-9]+")) {
                    ints[i] = Integer.parseInt(values[i]);
                }
            }
            context.write(new IntWritable(airlineId), new AirlineFlightDelaysWritable(airlineId, ints));
        }
    }

    /**
     * This reducer finds the top N records. TODO: allow N to be specified.
     */
    public static class CompareArrivalDelayReduce
            extends Reducer<IntWritable, AirlineFlightDelaysWritable, NullWritable, Text> {
        private Integer n;
        private TreeSet<AirlineFlightDelaysWritable> airlines;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            final Configuration conf = context.getConfiguration();
            this.n = conf.getInt("N", 10);

            // load AIRLINES lookup table.
            for (URI uri : context.getCacheArchives()) {
                if ("file".equals(uri.getScheme())) {
                    if (uri.getPath().endsWith("rita-static.zip")) {
                        LookupUtil.load(new File(uri.getPath()));
                    }
                }
            }

            // we initially sort the airlines by most flights (or miles)
            airlines = new TreeSet<AirlineFlightDelaysWritable>() {
                private static final long serialVersionUID = 1;

                @Override
                public Comparator<AirlineFlightDelaysWritable> comparator() {
                    return new Comparator<AirlineFlightDelaysWritable>() {
                        public int compare(AirlineFlightDelaysWritable x, AirlineFlightDelaysWritable y) {
                            return x.getNumFlights() - y.getNumFlights();
                        }
                    };
                }
            };
        }

        /**
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
         *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(IntWritable key, Iterable<AirlineFlightDelaysWritable> values,
                Reducer<IntWritable, AirlineFlightDelaysWritable, NullWritable, Text>.Context context)
                        throws IOException, InterruptedException {

            final int airlineId = key.get();
            AirlineFlightDelaysWritable w = new AirlineFlightDelaysWritable(airlineId);
            for (AirlineFlightDelaysWritable airline : values) {
                w.add(airline);
            }

            airlines.add(w);
            while (airlines.size() > 25) {
                airlines.remove(airlines.last());
            }
        }

        /**
         * Write final results.
         * 
         * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void cleanup(Reducer<IntWritable, AirlineFlightDelaysWritable, NullWritable, Text>.Context context)
                throws IOException, InterruptedException {

            // we now sort airlines by on-time arrival statistics
            final TreeSet<AirlineFlightDelaysWritable> delays = new TreeSet<>(airlines);

            for (AirlineFlightDelaysWritable delay : delays) {
                final int airlineId = delay.getAirlineId();
                if (LookupUtil.AIRLINES.containsKey(airlineId)) {
                    context.write(NullWritable.get(),
                            new Text(String.format("%7.3f %6.3f %s", delay.getMean() + 2 * delay.getStdDev(),
                                    delay.getMean(), LookupUtil.AIRLINES.get(airlineId).getName())));
                } else {
                    context.write(NullWritable.get(), new Text(String.format("%7.3f %6.3f (unknown: %d)",
                            delay.getMean() + 2 * delay.getStdDev(), delay.getMean(), airlineId)));
                }
            }
        }
    }
}