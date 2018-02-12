package ae;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce2 extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
		
		String result ="" ;
		Double sum = 0.0;
		String outGoing = ""; 
		
		// for each value, calculate score
		for (Text value: values) {
			String token[]= value.toString().trim().split("\t| |\\s",-1);
			if(token.length>0) {
				if(token[0].equals(">")) {
					outGoing = value.toString().substring(value.toString().indexOf(">")+1);
				}
				else {
//					result += "0="+token[0]+" 1="+token[1]+" 2="+token[2];
					String v = token[0].toString();
					if (token[1]!=null && token[2] != null && token[1].length()>0 && token[2].length()>0) {
						Double rate = Double.parseDouble(token[1]) / Double.parseDouble(token[2]);
//						Double rate = 0.0;
						sum += rate;
					}
				}
			}
		}
		
		//PR(u)=0.15 + 0.85 * Sum(PR(v)/L(v)
		Double scoreU = (0.15 + 0.85*(sum));
		
//		outGoing = outGoing.trim();
		
//		if(outGoing!=null && outGoing.length()>0) outGoing = " "+outGoing;
		
		result = scoreU +" "+outGoing.trim();
		context.write(key,new Text(result) );
	}
}