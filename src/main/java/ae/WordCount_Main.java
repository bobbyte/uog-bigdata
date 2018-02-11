package ae;


import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount_Main extends Configured implements Tool {


	
	private static final String OUTPUT_PATH = "intermediate_output";

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		conf.set("textinputformat.record.delimiter","\n\n");
		
		Job job = Job.getInstance(conf,"WordCount-main"+args[0]);
		
		job.setJarByClass(WordCount_Main.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/job1"));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);
		  /*
		   * Job 2
		   */
		  
		conf.set("textinputformat.record.delimiter","\n");
		
		Job job2 = Job.getInstance(conf, "Job 2");
		job2.setJarByClass(WordCount_Main.class);
	
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
	
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		  
		job2.setInputFormatClass(FileInputFormat.class);
		job2.setOutputFormatClass(FileOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[1] + "/job1"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/job2"));

		return job2.waitForCompletion(true) ? 0 : 1;
		 
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new WordCount_Main(), args));
	}
}