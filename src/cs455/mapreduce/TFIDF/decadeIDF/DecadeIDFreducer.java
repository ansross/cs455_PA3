package cs455.mapreduce.TFIDF.decadeIDF;

import java.io.IOException;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

//Calculates IDF for n-grams in a decade
public class DecadeIDFreducer extends Reducer<Text , n_gramInfo, Text, Text>{
	public void reduce(Text key, Iterable<n_gramInfo> values, Context context)
	throws IOException, InterruptedException{
		//track which books contain each n_gram
		HashMap<String, HashSet<String>> n_grams = new HashMap<String, HashSet<String>>();
		//ArrayList<String> n_grams = new ArrayList<String>();
		//get collection of all books in decade to know number
		//of documents in corpus
		ArrayList<n_gramInfo> valuesCopy = new ArrayList<n_gramInfo>();
		HashSet<String> books = new HashSet<String>();
		//get counts of all n-grams
		String output = "";
		for(n_gramInfo info: values){
			output=output.concat(info.toString()+'\n');
			valuesCopy.add(new n_gramInfo(info));
			String current_n_gram = info.get_n_gram();
			//add one 
			if(n_grams.containsKey(info.get_n_gram())){
				n_grams.get(current_n_gram).add(info.bookTitle.toString());
			}
			else{
				//TODO better!!
				
				n_grams.put(current_n_gram, new HashSet<String>());
				n_grams.get(current_n_gram).add(info.bookTitle.toString());
			}
			books.add(info.getBookTitle());
		}
		//calculate IDF for all n_grams

		double numDocsInCorpus = books.size();
		//for(n_gramInfo info: valuesCopy)
		//Iterator<Entry<String, HashSet<String>>> gramIt = n_grams.entrySet().iterator();
		//while(gramIt.hasNext()){
		//for(Map.Entry<String, HashSet<String>> entry: n_grams.entrySet())
		for(n_gramInfo info:valuesCopy){
			//String n_gram = entry.getKey();
			double idf = 
					Math.log10(numDocsInCorpus/(double)n_grams.get(info.get_n_gram()).size());
			output=output.concat(new IDFn_gramInfo(info,idf).toString()+'\n');
			//output = output.concat("info: "+info.toString()+'\n');
			
		}
		context.write(new Text(key+"\n"), new Text(output));
		
	}
}
