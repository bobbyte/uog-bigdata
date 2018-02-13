package ae;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce2 extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
		
		String result ="" ;
		Double sum = 0.0;
		String outGoing = ""; 
		
		// for each value, calculate score and get main link
		for (Text value: values) {
			String token[]= value.toString().trim().split("\t| |\\s",-1);
			if(token.length>0) {
				//case1: get outgoing link from key
				if(token[0].equals(">")) {
					
					outGoing = value.toString().substring(value.toString().indexOf(">")+1);
				}
				//case2: get score for key
				else {

					if (token[1]!=null && token[2] != null && token[1].length()>0 && token[2].length()>0) {
						
						Double scoreV_by_numlinkV = Double.parseDouble(token[1]) / Double.parseDouble(token[2]);
						sum += scoreV_by_numlinkV;
					}
				}
			}
		}
		
		//PR(u)=0.15 + 0.85 * Sum(PR(v)/L(v)
		Double scoreU = (0.15 + 0.85*(sum));
		
		//key = article_title , value = score + outgoing_from_key
		result = scoreU +" "+outGoing.trim();
		context.write(key,new Text(result) );
	}
}