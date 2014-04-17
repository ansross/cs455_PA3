package cs455.mapreduce.TFIDF.decadeIDF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import cs455.mapreduce.TFIDF.Util.Protocol;

public class DecadeIDFmapper extends Mapper<LongWritable, Text, Text, n_gramInfo>{
	public void map(LongWritable key, Text bookText,Context context) throws IOException, InterruptedException{
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String filename = fileSplit.getPath().getName();

		//extract year from filename
		String yearString = extractYear(filename);
		String decadeString = extractDecade(yearString);

		String line = bookText.toString();

		StringTokenizer tok = new StringTokenizer(line);

		ArrayList<String> n_gram = new ArrayList<String>();
		//extract n words for the first n-gram 
		for(int gram=1; gram<=Protocol.n; gram++){
			if(tok.hasMoreTokens()){
				n_gram.add(tok.nextToken());
			}
		}
		//if enough words to make at least one n-gram
		if(n_gram.size() == Protocol.n){
			//if enough words for an n-gram, add first n-gram
			String first_n_gram_string = get_n_gram(n_gram);
			n_gramInfo first_info = new n_gramInfo(filename,yearString,first_n_gram_string);
			context.write(new Text(decadeString), first_info);
			while(tok.hasMoreTokens()){
				n_gram.remove(0);
				n_gram.add(n_gram.size(), tok.nextToken());
				String n_gram_string = get_n_gram(n_gram);
				n_gramInfo info = new n_gramInfo(filename,yearString,n_gram_string);
				context.write(new Text(decadeString), info);
				
			}
			//extract n-gram
		}
		else{
			System.out.println("Not enough words");
		}

	}
	
	private String get_n_gram(ArrayList<String> grams){
		String n_gram_string="";
		for(int i=0; i<grams.size(); ++i){
			n_gram_string +=grams.get(i);
			if(i!=grams.size()-1){
				n_gram_string+=" ";
			}
		}
		return n_gram_string;
		
	}
	//consider BC a decade
	private String extractDecade(String yearString){
		String decadeString = null;
		//check for BC
		if(yearString.charAt(yearString.length()-1)=='C'){
			decadeString = "BC";
		}
		//take off one's place and to get decade aka 882 = 880's and 1994 = 1990's
		else{
			//will grab decade year, aka 1
			decadeString = yearString.substring(0, yearString.length()-1)+"0's";
		}
		return decadeString;
	}

	//REMEMBER BC!!!!!!! TODO
	private String extractYear(String filename){
		int indexOfYear=filename.indexOf('r');
		int indexOfYearEnd = filename.indexOf('.');
		String yearString = filename.substring(indexOfYear+1, indexOfYearEnd);
		return yearString;
	}

}
