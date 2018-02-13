package ae;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank_Main extends Configured implements Tool {



	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		/*
		 *  ***** Job1 ******* 
		 *  Map.class map article and outlink from input file along with Revision_id
		 *  Reduce.class select the lastest Revision_id, remove duplicates
		 *  ******************
		 */
		Configuration conf1 = getConf();
		conf1.set("textinputformat.record.delimiter","\n\n");
		
		Job job = Job.getInstance(conf1,"PageRank_main - Job1");
		
		job.setJarByClass(PageRank_Main.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/job1"));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
	    job.waitForCompletion(true);
	    
	    /*
	     * ***** Job 2 *********
	     * Calculate PageRank for each article
	     * result from each round will be stored in /job2_(numberOfIteration)
	     * *********************
	     */
	    
	    int numIter = Integer.parseInt(args[2]);
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
	    
	    /*
	     * ***** Job 3 *********
	     * Only have Mapper
	     * cut off the outlink line and display only article_title & pageRank
	     * *********************
	     */
	    
	    Configuration conf3 = getConf();
		conf3.set("textinputformat.record.delimiter","\n");
		
		Job job3 = Job.getInstance(conf1,"PageRank_main - Job3");
		
		job3.setJarByClass(PageRank_Main.class);
		
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
		
		Job job = Job.getInstance(conf,"PageRank_main - Job2");
		
		job.setJarByClass(PageRank_Main.class);
		
		job.setMapperClass(Map2.class);
		job.setReducerClass(Reduce2.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
	    return job.waitForCompletion(true);
	}
	public static void main(String[] args) throws Exception {
		
		// Check that the input is correct (number of iteration is positive integer)
		try{
			int numIter =Integer.parseInt(args[2]);
			if(numIter<=0) {
				System.err.println("********************************\nPlease input a positive integer \n********************************");
				System.exit(0);
			}
			System.out.println("********************************\nNumber of iteration is "+numIter+"\n********************************");
		}catch(Exception e) {
			System.err.println("********************************\nCaught Exception: " + e.getMessage());
			System.err.println("Please input a positive integer\n********************************");
			System.exit(0);
		}
		System.exit(ToolRunner.run(new Configuration(), new PageRank_Main(), args));
	}
}