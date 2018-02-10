package mywordcount;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyRecordReader extends RecordReader<LongWritable, Text>{

	private static final byte[] recordSeparator = "\n\n".getBytes();
	private FSDataInputStream fsin;
	private long start,end;
	private boolean stillInChunk = true;
	private DataOutputBuffer buffer = new DataOutputBuffer();
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		fsin.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return (float)(fsin.getPos()-start)/(end-start);
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = context.getConfiguration();
		Path path = split.getPath();
		org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
		
		fsin = fs.open(path);
		start = split.getStart();
		end = split.getStart() + split.getLength();
		fsin.seek(start);
		
		if(start != 0) readRecord(false);
		
	}
	
	

	private boolean readRecord(boolean withinBlock) throws IOException {
		// TODO Auto-generated method stub
		int i =0, b;
		while (true) {
			if ((b = fsin.read()) == -1) return false;
			if(withinBlock) buffer.write(b);
			if(b==recordSeparator[i]) {
				if(++i == recordSeparator.length) return fsin.getPos() < end;
			} else {
				i = 0;
			}
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(!stillInChunk) {
			return false;
		}
		boolean status = readRecord(true);
		value = new Text();
		value.set(buffer.getData(),0,buffer.getLength());
		key.set(fsin.getPos());
		buffer.reset();
		if(!status) stillInChunk = false;
				
		return false;
	}
	

}
