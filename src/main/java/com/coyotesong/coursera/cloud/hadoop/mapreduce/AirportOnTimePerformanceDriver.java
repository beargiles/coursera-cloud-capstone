package com.coyotesong.coursera.cloud.hadoop.mapreduce;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
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

import com.coyotesong.coursera.cloud.domain.AirlineInfo;
import com.coyotesong.coursera.cloud.hadoop.io.AirlineFlightDelaysWritable;
import com.coyotesong.coursera.cloud.hadoop.io.AirportsAndAirlineWritable;
import com.coyotesong.coursera.cloud.hadoop.mapreduce.lib.output.AirlineFlightDelaysOutputFormat;
import com.coyotesong.coursera.cloud.util.CSVParser;

/**
 * Hadoop driver that identifies the airlines with the best on-time arrival
 * performance at each airport.
 * 
 * See AirlineOnTimePerformanceDriver javadocs for discussion of "arrival
 * performance".
 *
 * @author bgiles
 */
public class AirportOnTimePerformanceDriver extends Configured implements Tool {
    private static final File ROOT = new File("/media/router/Documents/Coursera Cloud");

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
        job.setOutputKeyClass(AirportsAndAirlineWritable.class);
        job.setOutputValueClass(AirlineFlightDelaysWritable.class);

        job.setMapOutputKeyClass(AirportsAndAirlineWritable.class);
        job.setMapOutputValueClass(AirlineFlightDelaysWritable.class);

        job.setMapperClass(GatherArrivalDelayMap.class);
        job.setReducerClass(GatherArrivalDelayReduce.class);
        job.setCombinerClass(GatherArrivalDelayReduce.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setOutputFormatClass(AirlineFlightDelaysOutputFormat.class);

        job.setJarByClass(AirportOnTimePerformanceDriver.class);

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

        job.setJarByClass(AirportOnTimePerformanceDriver.class);

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
        return 1;

        // final Job jobB = setupSecondJob(new Path(args[2]), new
        // Path(args[1]));
        // return jobB.waitForCompletion(true) ? 1 : 0;
    }

    /**
     * The mapper reads one line of the CSV file and produces a
     * (airport/airline, delay) entry. The mapper output is designed to allow
     * the use of a combiner.
     */
    public static class GatherArrivalDelayMap
            extends Mapper<LongWritable, Text, AirportsAndAirlineWritable, AirlineFlightDelaysWritable> {
        private int airlineIdx = 5;
        private int originIdx = 8;
        private int destinationIdx = 11;
        private int arrDelayIdx = 17;
        private int cancelledIdx = 18;
        private int divertedIdx = 19;

        @Override
        protected void setup(
                Mapper<LongWritable, Text, AirportsAndAirlineWritable, AirlineFlightDelaysWritable>.Context context) {
            // TODO: retrieve indexes
        }

        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, AirportsAndAirlineWritable, AirlineFlightDelaysWritable>.Context context)
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
            final String originIdStr = values.get(originIdx);
            final String destinationIdStr = values.get(destinationIdx);
            String arrDelayStr = values.get(arrDelayIdx);
            final int idx = arrDelayStr.indexOf('.');
            if (idx > 0) {
                arrDelayStr = arrDelayStr.substring(0, idx);
            }
            if (airlineIdStr.matches("[0-9]+") && originIdStr.matches("[0-9]+") && destinationIdStr.matches("[0-9]+")
                    && arrDelayStr.matches("-?[0-9]+")) {
                final int airlineId = Integer.parseInt(airlineIdStr);
                final int originId = Integer.parseInt(originIdStr);
                final int destinationId = Integer.parseInt(destinationIdStr);
                final int delay = Integer.parseInt(arrDelayStr);
                final AirportsAndAirlineWritable outKey = new AirportsAndAirlineWritable(originId, destinationId,
                        airlineId);
                context.write(outKey, new AirlineFlightDelaysWritable(airlineId, delay));
            }
        }
    }

    /**
     * The reducer adds up the delays for each pairs of airports and airline
     */
    public static class GatherArrivalDelayReduce extends
            Reducer<AirportsAndAirlineWritable, AirlineFlightDelaysWritable, AirportsAndAirlineWritable, AirlineFlightDelaysWritable> {
        @Override
        protected void reduce(AirportsAndAirlineWritable key, Iterable<AirlineFlightDelaysWritable> values,
                Reducer<AirportsAndAirlineWritable, AirlineFlightDelaysWritable, AirportsAndAirlineWritable, AirlineFlightDelaysWritable>.Context context)
                        throws IOException, InterruptedException {
            final AirlineFlightDelaysWritable w = new AirlineFlightDelaysWritable(key.getAirlineId());
            for (AirlineFlightDelaysWritable value : values) {
                w.add(value);
            }
            context.write(key, w);
        }
    }

    /**
     * This mapper loads the airline id and on-time arrival delay information
     */
    public static class CompareArrivalDelayMap
            extends Mapper<Text, Text, AirportsAndAirlineWritable, AirlineFlightDelaysWritable> {

        @Override
        protected void map(Text key, Text value,
                Mapper<Text, Text, AirportsAndAirlineWritable, AirlineFlightDelaysWritable>.Context context)
                        throws IOException, InterruptedException {
            final int[] ints = new int[4];

            // compute outKey
            String[] values = key.toString().split(",");
            for (int i = 0; i < 3; i++) {
                if (values[i].matches("?[0-9]+")) {
                    ints[i] = Integer.parseInt(values[i]);
                }
            }
            final AirportsAndAirlineWritable outKey = new AirportsAndAirlineWritable(ints[0], ints[1], ints[2]);

            // compute outValue
            values = key.toString().split(",");
            for (int i = 0; i < ints.length; i++) {
                if (values[i].matches("-?[0-9]+")) {
                    ints[i] = Integer.parseInt(values[i]);
                }
            }
            final AirlineFlightDelaysWritable outValue = new AirlineFlightDelaysWritable(outKey.getAirlineId(), ints);

            // TODO: add field validation?

            context.write(outKey, outValue);
        }
    }

    /**
     * This reducer finds the top N records. TODO: allow N to be specified.
     */
    public static class CompareArrivalDelayReduce
            extends Reducer<AirportsAndAirlineWritable, AirlineFlightDelaysWritable, NullWritable, Text> {
        private Integer n;
        private TreeSet<AirlineFlightDelaysWritable> airlines;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            final Configuration conf = context.getConfiguration();
            this.n = conf.getInt("N", 10);

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
        protected void reduce(AirportsAndAirlineWritable key, Iterable<AirlineFlightDelaysWritable> values,
                Reducer<AirportsAndAirlineWritable, AirlineFlightDelaysWritable, NullWritable, Text>.Context context)
                        throws IOException, InterruptedException {

            AirlineFlightDelaysWritable w = new AirlineFlightDelaysWritable(key.getAirlineId());
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
        protected void cleanup(Reducer<AirportsAndAirlineWritable, AirlineFlightDelaysWritable, NullWritable, Text>.Context context)
                throws IOException, InterruptedException {

            final Map<Integer, AirlineInfo> air = new HashMap<>();
            try (Reader r = new FileReader(new File(ROOT, "485012853_T_CARRIER_DECODE.csv"))) {
                for (CSVRecord record : CSVFormat.EXCEL.parse(r)) {
                    String id = record.get(0);
                    if (id.matches("[0-9]+")) {
                        AirlineInfo info = new AirlineInfo(Integer.valueOf(id), record.get(3));
                        air.put(info.getAirlineId(), info);
                    }
                }
            }

            // we now sort airlines by on-time arrival statistics
            final TreeSet<AirlineFlightDelaysWritable> delays = new TreeSet<>(airlines);

            for (AirlineFlightDelaysWritable delay : delays) {
                final int airlineId = delay.getAirlineId();
                if (air.containsKey(airlineId)) {
                    context.write(NullWritable.get(), new Text(String.format("%7.3f %6.3f %s",
                            delay.getMean() + 2 * delay.getStdDev(), delay.getMean(), air.get(airlineId).getName())));
                } else {
                    context.write(NullWritable.get(), new Text(String.format("%7.3f %6.3f (unknown: %d)",
                            delay.getMean() + 2 * delay.getStdDev(), delay.getMean(), airlineId)));
                }
            }
        }
    }
}
