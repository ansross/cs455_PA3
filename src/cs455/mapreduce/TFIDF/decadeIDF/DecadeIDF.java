package cs455.mapreduce.TFIDF.decadeIDF;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cs455.mapreduce.readinglevel.FeatureCounter;
import cs455.mapreduce.readinglevel.LevelsWritable;
import cs455.mapreduce.readinglevel.ReadingLevel;
import cs455.mapreduce.readinglevel.ReadingLevelMapper;
import cs455.mapreduce.readinglevel.ReadingLevelReducer;

public class DecadeIDF {
	public static void main(String[] args) throws ClassNotFoundException, 
	InterruptedException, IOException {
		Configuration conf = new Configuration();
		Job IDFjob  = Job.getInstance(conf);
		IDFjob.setJarByClass(DecadeIDF.class);
		IDFjob.setJobName("Decade IDF calculation");
		FileSystem fs = FileSystem.get(conf);
		FileStatus [] status_list = fs.listStatus(new Path(args[0]));
		if(status_list != null){
			for(FileStatus status: status_list){
				FileInputFormat.addInputPath(IDFjob, status.getPath());
			}
		}
		//FileInputFormat.setInputPaths(IDFjob, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(IDFjob,  new Path(args[1]));;
		IDFjob.setMapperClass(DecadeIDFmapper.class);
		IDFjob.setReducerClass(DecadeIDFreducer.class);
		IDFjob.setOutputValueClass(Text.class);
		IDFjob.setOutputKeyClass(Text.class);
		IDFjob.setMapOutputValueClass(n_gramInfo.class);
		IDFjob.setMapOutputKeyClass(Text.class);
		System.exit(IDFjob.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
