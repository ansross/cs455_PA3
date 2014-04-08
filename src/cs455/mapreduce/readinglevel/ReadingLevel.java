package cs455.mapreduce.readinglevel;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReadingLevel {
	public static void main(String[] args) throws ClassNotFoundException, 
	InterruptedException, IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(ReadingLevel.class);
		job.setJobName("Reading Level");
		FileSystem fs = FileSystem.get(conf);
		FileStatus [] status_list = fs.listStatus(new Path(args[0]));
		if(status_list != null){
			for(FileStatus status: status_list){
				FileInputFormat.addInputPath(job, status.getPath());
			}
		}
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));;
		job.setMapperClass(ReadingLevelMapper.class);
		job.setReducerClass(ReadingLevelReducer.class);
		job.setOutputValueClass(LevelsWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FeatureCounter.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
