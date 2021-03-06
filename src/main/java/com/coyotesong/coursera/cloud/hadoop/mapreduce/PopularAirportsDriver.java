package com.coyotesong.coursera.cloud.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

import com.coyotesong.coursera.cloud.domain.FlightInfo;
import com.coyotesong.coursera.cloud.hadoop.io.AirportFlightsWritable;
import com.coyotesong.coursera.cloud.util.CSVParser;
import com.coyotesong.coursera.cloud.util.LookupUtil;

/**
 * Hadoop driver that identifies the most popular airports by total number of
 * arrivals and departures.
 * 
 * @author bgiles
 */
public class PopularAirportsDriver extends Configured implements Tool {

    /**
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        final Job jobA = setupFirstJob(new Path(args[0]), new Path(args[2]));
        boolean success = jobA.waitForCompletion(true);
        if (!success) {
            return 0;
        }

        // should get from argument list.
        URI ritaStatic = null;
        try {
            ritaStatic = Thread.currentThread().getContextClassLoader().getResource("rita-static.zip").toURI();
        } catch (URISyntaxException e) {
            // should never happen
            throw new AssertionError(e);
        }

        final Job jobB = setupSecondJob(new Path(args[2]), new Path(args[1]), ritaStatic);
        return jobB.waitForCompletion(true) ? 1 : 0;
    }

    /**
     * Set up first job - it reads input files and creates a file containing the
     * airport ID and total number of flights.
     * 
     * @param input
     * @param output
     * @return
     */
    public Job setupFirstJob(Path input, Path output) throws IOException {
        final Job job = Job.getInstance(this.getConf(), "Airport Count");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(GatherFlightsMap.class);
        job.setReducerClass(GatherFlightsReduce.class);
        job.setCombinerClass(GatherFlightsReduce.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(PopularAirportsDriver.class);

        return job;
    }

    /**
     * Set up second job - it reads file containing airport ID and number of
     * flights and identifies the most popular airports.
     */
    public Job setupSecondJob(Path input, Path output, URI ritaStatic) throws IOException {
        final Job job = Job.getInstance(this.getConf(), "Top Airports");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(AirportFlightsWritable.class);

        job.setMapperClass(TopAirportsMap.class);
        job.setReducerClass(TopAirportsReduce.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(PopularAirportsDriver.class);

        job.addCacheFile(ritaStatic);

        return job;
    }

    /**
     * The mapper reads one line of the CSV file and produces a (airport, 1)
     * entry for both origin and destination. The total number of arrivals and
     * departures should be roughly the same for every airport.
     */
    public static class GatherFlightsMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
                        throws IOException, InterruptedException {

            // skip first line
            final List<String> values = CSVParser.parse(value.toString());
            if (values.get(0).matches("[0-9]+")) {
                final FlightInfo info = FlightInfo.Builder.build(values);

                if (!info.isCancelled() && !info.isDiverted()) {
                    context.write(new IntWritable(info.getDestAirportId()), ONE);
                }
            }
        }
    }

    /**
     * The reducer adds up the number of flights at each airport.
     */
    public static class GatherFlightsReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
                Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
                        throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    /**
     * This mapper flips the airport id and number of flights.
     */
    public static class TopAirportsMap extends Mapper<Text, Text, NullWritable, AirportFlightsWritable> {

        @Override
        protected void map(Text key, Text value,
                Mapper<Text, Text, NullWritable, AirportFlightsWritable>.Context context)
                        throws IOException, InterruptedException {
            final int airportId = Integer.parseInt(key.toString());
            final int flights = Integer.parseInt(value.toString());
            context.write(NullWritable.get(), new AirportFlightsWritable(airportId, flights));
        }
    }

    /**
     * This reducer finds the top N records.
     */
    public static class TopAirportsReduce extends Reducer<NullWritable, AirportFlightsWritable, IntWritable, Text> {
        private Integer n;
        private TreeSet<AirportFlightsWritable> airports;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            final Configuration conf = context.getConfiguration();
            this.n = conf.getInt("N", 10);

            // load AIRPORTS lookup table.
            for (URI uri : context.getCacheFiles()) {
                if ("file".equals(uri.getScheme())) {
                    if (uri.getPath().endsWith("rita-static.zip")) {
                        LookupUtil.load(new File(uri.getPath()));
                    }
                }
            }
            
            airports = new TreeSet<>(new Comparator<AirportFlightsWritable>(){
                public int compare(AirportFlightsWritable p, AirportFlightsWritable q) {
                    int l = p.getFlights();
                    int r = q.getFlights();
                    if (l < r) {
                        return -1;
                    } else if (l > r) {
                        return 1;
                    }
                    return 0;
                }
            });
        }

        /**
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
         *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(NullWritable key, Iterable<AirportFlightsWritable> values,
                Reducer<NullWritable, AirportFlightsWritable, IntWritable, Text>.Context context)
                        throws IOException, InterruptedException {

            // create copy of input value since hadoop seems to be reusing
            // values. Otherwise all entries in the set have the same value.
            for (AirportFlightsWritable airport : values) {
                airports.add(new AirportFlightsWritable(airport.getAirportId(), airport.getFlights()));
                if (airports.size() > n) {
                    airports.remove(airports.first());
                }
            }
        }

        /**
         * Write final results.
         * 
         * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void cleanup(Reducer<NullWritable, AirportFlightsWritable, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {

            List<AirportFlightsWritable> list = new ArrayList<>(airports);
            Collections.reverse(list);
            
            for (AirportFlightsWritable airport : list) {
                final int flights = airport.getFlights();
                final int airportId = airport.getAirportId();
                if (LookupUtil.AIRPORTS.containsKey(airportId)) {
                    context.write(new IntWritable(flights),
                            new Text(String.format("%s", LookupUtil.AIRPORTS.get(airportId).getName())));
                } else {
                    context.write(new IntWritable(flights), new Text(String.format("unknown (%d)", airportId)));
                }
            }
        }
    }
}
