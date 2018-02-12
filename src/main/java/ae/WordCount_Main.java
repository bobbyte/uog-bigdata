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
		conf1.set("textinputformat.record.delimiter","\n");
		
		Job job = Job.getInstance(conf1,"WordCount-main"+args[0]);
		
		job.setJarByClass(WordCount_Main.class);
		
		job.setMapperClass(Map2.class);
		job.setCombinerClass(Reduce2.class);
		job.setReducerClass(Reduce2.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/job1"));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//ControlledJob controlledJob1 = new ControlledJob(conf1);
	    //controlledJob1.setJob(job);
	    
	    //jobControl.addJob(controlledJob1);

	    
		//job.setOutputFormatClass(TextOutputFormat.class);
		//return job.waitForCompletion(true);
		  /*
		   * Job 2
		   */
		  
//		Configuration conf2 = getConf();
//		conf2.set("textinputformat.record.delimiter","\n");
//		
//		@SuppressWarnings("deprecation")
//		Job job2 = new Job(conf2,"JOBBOBBBBB");
//		//Job job2 = Job.getInstance(conf2, "Job 2");
//		job2.setJarByClass(WordCount_Main.class);
//	
//		
//		FileInputFormat.setInputPaths(job2, new Path(args[0] + "/job1"));
//		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/job2"));
//		
//		job2.setMapperClass(Map2.class);
//		job2.setCombinerClass(Reduce2.class);
//		job2.setReducerClass(Reduce2.class);
//	
//		job2.setOutputKeyClass(Text.class);
//		job2.setOutputValueClass(Text.class);
//		
//		 
//
//		job2.setInputFormatClass(KeyValueTextInputFormat.class);
//		job2.setOutputFormatClass(FileOutputFormat.class);
//
//	    ControlledJob controlledJob2 = new ControlledJob(conf2);
//	    controlledJob2.setJob(job2);
//		
//
//		// make job2 dependent on job1
//	    controlledJob2.addDependingJob(controlledJob1); 
//	    // add the job to the job control
//	    jobControl.addJob(controlledJob2);
//	    Thread jobControlThread = new Thread(jobControl);
//	    jobControlThread.start();
//	    
//	    
//		//return job2.waitForCompletion(true) ? 0 : 1;
//		 
//	    while (!jobControl.allFinished()) {
//	        System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
//	        System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
//	        System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
//	        System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
//	        System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
//		    try {
//		        Thread.sleep(5000);
//		    } catch (Exception e) {
//	
//	        }
//	    }
//	    System.exit(0);  
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new WordCount_Main(), args));
	}
}