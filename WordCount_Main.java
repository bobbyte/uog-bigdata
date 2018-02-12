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
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount_Main extends Configured implements Tool {



	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf1 = getConf();
		conf1.set("textinputformat.record.delimiter","\n\n");
		
		Job job = Job.getInstance(conf1,"WordCount-main"+args[0]);
		
		job.setJarByClass(WordCount_Main.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/job1"));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
	    job.waitForCompletion(true);
	    
	    int numIter = 3;
	    String currentPath = "";
	    String nextPath = "";
	    boolean isJobFinish = false;   
	    for(int i= 0; i<numIter; i++) {
	    	if (i == 0) {
	    		currentPath = args[1] + "/job1"  ;
	    	}
	    	else currentPath = args[1] + "/job2_"+ (i+0)  ;
	    	
	    	nextPath = args[1] + "/job2_"+ (i+1) ;
	    	isJobFinish = scoreCalculation(currentPath, nextPath);
	    }
	    
	    Configuration conf3 = getConf();
		conf3.set("textinputformat.record.delimiter","\n");
		
		Job job3 = Job.getInstance(conf1,"WordCount-main"+args[0]);
		
		job3.setJarByClass(WordCount_Main.class);
		
		job3.setMapperClass(Map3.class);
		
		FileInputFormat.addInputPath(job3, new Path(args[1]+"/job2_"+ (numIter+0)));
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/job3"));
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
	
		isJobFinish = job3.waitForCompletion(true);
	   
	    if(isJobFinish) return 0;
	    return 1;
	}
	
	public boolean scoreCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException{
	
		Configuration conf = getConf();
		conf.set("textinputformat.record.delimiter","\n");
		
		Job job = Job.getInstance(conf,"WordCount-main-job2");
		
		job.setJarByClass(WordCount_Main.class);
		
		job.setMapperClass(Map2.class);
		job.setReducerClass(Reduce2.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
	    return job.waitForCompletion(true);
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new WordCount_Main(), args));
	}
}