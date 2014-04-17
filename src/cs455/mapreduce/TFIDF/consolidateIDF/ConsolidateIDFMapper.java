package cs455.mapreduce.TFIDF.consolidateIDF;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import cs455.mapreduce.TFIDF.decadeIDF.IDFn_gramInfo;
import cs455.mapreduce.TFIDF.decadeIDF.n_gramInfo;

public class ConsolidateIDFMapper extends 
Mapper<LongWritable, Text, Text, IDFn_gramInfoCount>{
	public void map(LongWritable key, Text IDFRawText, Context context) 
			throws IOException, InterruptedException{
		FileSplit fileSplit = (FileSplit) context.getInputSplit();

		String line = IDFRawText.toString();

		StringTokenizer tok = new StringTokenizer(line);
		while(tok.hasMoreTokens()){
			String token = tok.nextToken();
			String[] tokens = token.split(",");
			//if just year, don't do anything with it
			if(tokens.length ==4){
				n_gramInfo gramInfo = new n_gramInfo(tokens[0],tokens[1],tokens[2]);
				IDFn_gramInfo info = new IDFn_gramInfo(gramInfo, Double.parseDouble(tokens[3]));
				IDFn_gramInfoCount infoCount = new IDFn_gramInfoCount(info, 1);
				context.write(new Text(tokens[0]), infoCount);
			}
		}

	}
}
