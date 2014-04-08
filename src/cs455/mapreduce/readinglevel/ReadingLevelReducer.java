package cs455.mapreduce.readinglevel;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReadingLevelReducer extends 
Reducer <Text, FeatureCounter, Text, LevelsWritable>{
	public void reduce(Text key, Iterable<FeatureCounter> values,
			Context context) throws IOException, InterruptedException{
		double totalSyllables=0;
		double totalWords=0;
		double totalSentences=0;
		
		for(FeatureCounter fc: values){
			if(fc.getType().equals(new IntWritable(Protocol.SENTENCE))){
				totalSentences+=fc.getCount().get();
			}
			else if(fc.getType().equals(new IntWritable(Protocol.WORD))){
				totalWords += fc.getCount().get();
			}
			else if(fc.getType().equals(new IntWritable(Protocol.SYLLABLE))){
				totalSyllables+=fc.getCount().get();
			}
		}
		double readingEase = 206.835-1.015*(totalWords/totalSentences)-
				84.6*(totalSyllables/totalWords);
		double gradeLevel = 0.39*(totalWords/totalSentences)+ 
				11.8*(totalSyllables/totalWords)-15.59;
		
		LevelsWritable output = new LevelsWritable(readingEase, gradeLevel);
		context.write(key, output);
		
	}

}
