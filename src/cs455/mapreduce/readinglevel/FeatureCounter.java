package cs455.mapreduce.readinglevel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class FeatureCounter implements Writable{
	private IntWritable type;
	private IntWritable count;
	
	public IntWritable getType(){
		return type;
	}
	
	public IntWritable getCount(){
		return count;
	}
	
	public FeatureCounter(){
		type = new IntWritable();
		count = new IntWritable();
	}
	
	public FeatureCounter(IntWritable typeArg, IntWritable countArg){
		this.type=typeArg;
		this.count=countArg;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		type.readFields(in);
		count.readFields(in);
		
	}

	@Override
	public void write(DataOutput dataOut) throws IOException {
		type.write(dataOut);
		count.write(dataOut);
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public String toString(){
		return type + " " + count;
	}
	

}
