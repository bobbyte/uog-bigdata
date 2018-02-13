package ae;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Map2 extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
	private Text output = new Text();
	private Text word = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
		String result = "";
		String line = value.toString();
		String token[] = new String[0];

			
		// Split page line to array token[]
		token = line.trim().split("\\t| ", -1);

		if(token.length>2) {
			
			for(int i=2; i<token.length; i++) {
				// key = article_title_u
				word.set(token[i]);
				// value = article_title_v + score_v + numlinks_v ; where v -> u
				result = token[0]+ " " + token[1] +" "+ (token.length-2);
	
				output.set(result);
				context.write(word, output);
			
			}
			
			//write key = article_tile_v , value = "> " + outgoing_from_key
			result = "> "+ line.substring(line.indexOf(" ")+1);
			word.set(token[0]);
			output.set(result);
			context.write(word, output);
		}
	}
		
	

}