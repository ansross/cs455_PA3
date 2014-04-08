package cs455.mapreduce.readinglevel;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import sun.awt.SunHints.Value;
public class ReadingLevelMapper extends 
Mapper<LongWritable, Text, Text, FeatureCounter>{
	//one sentence per map?????????
	public void map(LongWritable key, Text bookTextValue, Context context) throws IOException, InterruptedException{
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String filename = fileSplit.getPath().getName();
		
		String line = bookTextValue.toString();
		System.out.println("Mapper value: " + line);
		
		StringTokenizer tok = new StringTokenizer(line);
		while(tok.hasMoreTokens()){
			//count this as one word
			//count syllable of word
			//check if end of sentence
			String token = tok.nextToken();
			FeatureCounter word = new FeatureCounter(
					new IntWritable(Protocol.WORD), new IntWritable(1));
			context.write(new Text(filename), word);
			
			int syls = CountSyllables.countSyllables(token);
			FeatureCounter syllable = new FeatureCounter(
					new IntWritable(Protocol.SYLLABLE), new IntWritable(syls));
			context.write(new Text(filename), syllable);
		}
		//when end of sentence, output that there was a sentence
		FeatureCounter sentence = new FeatureCounter(
				new IntWritable(Protocol.SENTENCE), new IntWritable(1));
		context.write(new Text(filename), sentence);
	}
	
}
