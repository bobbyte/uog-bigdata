package ae;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String result ="" ;
		
		int rev_id = 0;
		
		// for each value, get the highest Rev_id
		for (Text value: values) {
			
			String value_string= value.toString().trim();
			
			if(!value_string.equals("") && value_string.length()>0) {
				int temp_rev_id =Integer.parseInt(value_string.substring(0, value_string.indexOf(" ")));
				if(temp_rev_id > rev_id) {
					rev_id = temp_rev_id;
					result = value_string;
			
				}
		
			}
		}
		
		if(rev_id!=0) {
			// remove duplicated outgoing link
			String linksToPagesDup = result.substring(result.indexOf(" ")+1).trim();
			String linksToPages = "";
			String split[] = linksToPagesDup.split("\t| ",-1);
			int num = split.length;
			
			Set<String> set = new HashSet<String>();

			
			for(int i = 0; i < num; i++){
				String token = split[i].trim();
				if(token!= null&&token.length()>0) {
					set.add(token);
				}
			}
			
			Iterator<String> link = set.iterator();
			while(link.hasNext()) {
			  linksToPages += " "+link.next();
			}

			//final_value = "<initial_score> <linksToPage>" 
			Text final_value = new Text("1 "+linksToPages.trim()+"\n");

			context.write(key, final_value);
		}
		
	}
}