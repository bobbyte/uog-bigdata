package ae;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Map3 extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
	private Text output = new Text();
	private Text word = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
		String line = value.toString();
		String token[] = new String[0];

			
		// Split page line to array token[]
		token = line.trim().split("\\t| ", -1);


		word.set(token[0]);
		output.set(token[1]);
		context.write(word, output);
		}
	}
		
	

