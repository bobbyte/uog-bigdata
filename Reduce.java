package ae;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
			
			Iterator<String> page = set.iterator();
			while(page.hasNext()) {
			  linksToPages += " "+page.next();
			}
//			linksToPages = linksToPages.trim();
			Text sums = new Text("1 "+linksToPages.trim()+"\n");
//			if (linksToPages!= null && linksToPages.length() >0) sums.set("1 "+linksToPages+"\n");
//			else sums.set("1\n");
			context.write(key, sums);
		}
		
	}
}