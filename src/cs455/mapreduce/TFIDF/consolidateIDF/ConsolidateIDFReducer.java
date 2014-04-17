package cs455.mapreduce.TFIDF.consolidateIDF;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import cs455.mapreduce.TFIDF.decadeIDF.IDFn_gramInfo;
import cs455.mapreduce.TFIDF.decadeIDF.n_gramInfo;

public class ConsolidateIDFReducer extends Reducer<Text , IDFn_gramInfoCount, Text, Text>{
	public void reduce(Text key, Iterable<IDFn_gramInfoCount> values, Context context)
	throws IOException, InterruptedException{
		String output = "";
		HashMap<String, Integer> n_gramCount = new HashMap<String, Integer>();
		HashMap<String, IDFn_gramInfoCount> n_gramInfos = 
				new HashMap<String, IDFn_gramInfoCount>();
		for(IDFn_gramInfoCount infoCount:values){
			String n_gram = infoCount.getInfo().getn_gram();
			if(n_gramCount.containsKey(n_gram)){
				n_gramCount.put(n_gram, 
					n_gramCount.get(infoCount.getInfo().getn_gram())+
					infoCount.getCount());
			}
			else{
				n_gramCount.put(n_gram, 
						infoCount.getCount());
			}
		}
		for(Map.Entry<String, Integer> n_gram_entry : n_gramCount.entrySet()){
			IDFn_gramInfoCount infoCount = n_gramInfos.get(n_gram_entry.getKey());
			infoCount.updateCount(n_gram_entry.getValue());
			output = output.concat(n_gram_entry.getKey()+"   "+infoCount.toString()+'\n');
		}
		context.write(new Text(key.toString()+'\n'), new Text(output));
	}

}
