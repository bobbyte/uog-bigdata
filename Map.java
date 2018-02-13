package ae;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
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
					result= line[i].substring(line[i].indexOf(" ")+1).trim();
				}
				break; //skip the rest of article
			}
			
			
		
		}
		if(token.length>0) {
			// key = "Article_name" 
			word.set(token[3]);
			// value = "Revision" + "MAIN"
			result = token[2]+" "+result;

			output.set(result);
			context.write(word, output);
		}
	}
		
	

}