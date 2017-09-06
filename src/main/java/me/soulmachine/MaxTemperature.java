package me.soulmachine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxTemperature {

    static class MaxTeperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        //missing temperature value in climate data
        private static final int MISSING = 9999;

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{

            String line = value.toString();

            //extract year from climate data line
            String year = line.substring(15, 19);

            int airTemperature;

            if(line.charAt(87) == '+'){ //ParseInt doesn't accept leading plus sign
                airTemperature = Integer.parseInt(line.substring(88, 92));
            } else {
                airTemperature = Integer.parseInt(line.substring(87, 92));
            }

            //check if climate data is OK
            String quality = line.substring(92, 93);

            if(airTemperature != MISSING && quality.matches("[01459]")){
                context.write(new Text(year), new IntWritable(airTemperature));
            }
        }
    }

    static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int MaxValue = Integer.MIN_VALUE;

            for(IntWritable value : values){
                MaxValue = Math.max(MaxValue, value.get());
            }

            context.write(key, new IntWritable(MaxValue));
        }
    }

    public static void main(String[] args) throws Exception{

        //check if arguments correct
        if(args.length != 2){
            System.err.println("Usage: NewMaxTemperature <input path> <output path>");
            System.exit(-1);
        }


        // Create a new Job
        Job job = new Job(new Configuration());
        job.setJarByClass(MaxTemperature.class);

        // Specify various job-specific parameters
        job.setJobName("Max temperature");

        job.setMapperClass(MaxTemperature.MaxTeperatureMapper.class);
        job.setReducerClass(MaxTemperature.MaxTemperatureReducer.class);
        job.setCombinerClass(MaxTemperature.MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        // Submit the job, then poll for progress until the job is complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);




    }

}
