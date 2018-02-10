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

	static class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
		private Text output = new Text();
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			
			String result = "";
			String article = value.toString();
			String token[] = new String[0];
			
			
			// Split article into line
			String[] line= article.split("\n", -1);
			for(int i=0; i<line.length;i++) {
				
				// Split REVISION line to array token[]
				if(line[i].startsWith("REVISION")) {
					token = line[i].split(" ", -1);
				}
				
				// Collect MAIN line
				if(line[i].startsWith("MAIN")) {
					if(line[i].length()>0) {
						result= line[i].substring(line[i].indexOf(" ")+1);
					}
					break; //skip the rest of article
				}
				
				
			
			}
			if(token.length>0) {
				// key = "Article_name" 
				word.set(token[3]);
				// value = "Revision" + "MAIN"
				result = token[2]+" "+ result;

				output.set(result);
				context.write(word, output);
			}
		}
			
		
	
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String result ="" ;
			int rev_id = 0;
			// for each value, get the highest Rev_id
			for (Text value: values) {
				String temp= value.toString();
				int temp_rev_id =Integer.parseInt(temp.substring(0, temp.indexOf(" ")));
				if(temp_rev_id > rev_id) {
					rev_id = temp_rev_id;
					result = temp;
				}
			}
			/*	THINGS TO DO
			 *  - Split MAIN into many output
			 *  - check output of first job = input of remaining jobs
			 * 
			 * 
			 */
			if(rev_id!=0) {
				Text sums = new Text(result);
				context.write(key, sums);
			}
			
		}
	}

	@Override
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
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new WordCount_Main(), args));
	}
}