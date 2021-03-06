package ae;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String result ="" ;
		int rev_id = 0;
		// for each value, get the highest Rev_id
		for (Text value: values) {
			String temp= value.toString();
			if(!temp.equals("")) {
				int temp_rev_id =Integer.parseInt(temp.substring(0, temp.indexOf(" ")));
				if(temp_rev_id > rev_id) {
					rev_id = temp_rev_id;
					result = temp;
			
				}
		
			}
		}
		
		if(rev_id!=0) {
			Text sums = new Text("1 "+result.substring(result.indexOf(" ")+1).trim()+"\n");
			context.write(key, sums);
		}
		
	}
}