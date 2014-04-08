package cs455.mapreduce.readinglevel;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
public class LevelsWritable implements Writable{
	DoubleWritable FleschReadingEase;
	DoubleWritable FleschKincaidGradeLevel;
	
	public LevelsWritable(){
		FleschReadingEase = new DoubleWritable();
		FleschKincaidGradeLevel = new DoubleWritable();
	}
	
	public LevelsWritable(double readingEase, double gradeLevel){
		this.FleschReadingEase = new DoubleWritable(readingEase);
		this.FleschKincaidGradeLevel = new DoubleWritable(gradeLevel);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		FleschReadingEase.write(out);
		FleschKincaidGradeLevel.write(out);
		// TODO Auto-generated method stub
		
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		FleschReadingEase.readFields(in);
		FleschKincaidGradeLevel.readFields(in);
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public String toString(){
		return ("Reading Ease: "+FleschReadingEase+
				"\tGrade Level: "+FleschKincaidGradeLevel);
			}

}
