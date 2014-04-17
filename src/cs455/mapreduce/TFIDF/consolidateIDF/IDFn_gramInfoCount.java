package cs455.mapreduce.TFIDF.consolidateIDF;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import cs455.mapreduce.TFIDF.decadeIDF.IDFn_gramInfo;

public class IDFn_gramInfoCount implements Writable{
	private IDFn_gramInfo info;
	private IntWritable count;
	
	public void updateCount(int newCount){
		count = new IntWritable(newCount);
	}
	
	public int getCount(){
		return count.get();
	}
	
	public IDFn_gramInfo getInfo(){
		return info;
	}
	
	public IDFn_gramInfoCount(){
		info = new IDFn_gramInfo();
		count = new IntWritable();
	}
	
	public IDFn_gramInfoCount(IDFn_gramInfo infoArg, int countArg){
		info = new IDFn_gramInfo(infoArg);
		count = new IntWritable(countArg);
	}

	public IDFn_gramInfoCount(IDFn_gramInfoCount infoCount) {
		info = new IDFn_gramInfo(infoCount.getInfo());
		count = new IntWritable(infoCount.getCount());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		info.readFields(in);
		count.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		info.write(out);
		count.write(out);
		
	}
	
	public String toString(){
		return info.toString()+",count: "+count;
	}
	
	
}
