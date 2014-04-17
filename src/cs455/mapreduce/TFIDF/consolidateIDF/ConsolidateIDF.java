package cs455.mapreduce.TFIDF.consolidateIDF;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cs455.mapreduce.TFIDF.decadeIDF.DecadeIDF;
import cs455.mapreduce.TFIDF.decadeIDF.DecadeIDFmapper;
import cs455.mapreduce.TFIDF.decadeIDF.DecadeIDFreducer;
import cs455.mapreduce.TFIDF.decadeIDF.n_gramInfo;

public class ConsolidateIDF {
	public static void main(String[] args) throws ClassNotFoundException, 
	InterruptedException, IOException {
		Configuration conf = new Configuration();
		Job IDFConsoljob  = Job.getInstance(conf);
		IDFConsoljob.setJarByClass(ConsolidateIDF.class);
		IDFConsoljob.setJobName("IDF consolidations");
		FileSystem fs = FileSystem.get(conf);
		FileStatus [] status_list = fs.listStatus(new Path(args[0]));
		if(status_list != null){
			for(FileStatus status: status_list){
				FileInputFormat.addInputPath(IDFConsoljob, status.getPath());
			}
		}
		//FileInputFormat.setInputPaths(IDFjob, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(IDFConsoljob,  new Path(args[1]));;
		IDFConsoljob.setMapperClass(ConsolidateIDFMapper.class);
		IDFConsoljob.setReducerClass(ConsolidateIDFReducer.class);
		IDFConsoljob.setOutputValueClass(Text.class);
		IDFConsoljob.setOutputKeyClass(Text.class);
		IDFConsoljob.setMapOutputValueClass(IDFn_gramInfoCount.class);
		IDFConsoljob.setMapOutputKeyClass(Text.class);
		System.exit(IDFConsoljob.waitForCompletion(true) ? 0 : 1);
		
	}

}
