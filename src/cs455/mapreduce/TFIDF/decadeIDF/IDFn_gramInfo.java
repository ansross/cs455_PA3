package cs455.mapreduce.TFIDF.decadeIDF;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class IDFn_gramInfo implements Writable{
	Text bookTitle;
	Text year;
	Text n_gram;
	DoubleWritable decadeIDF;
	
	public String getn_gram(){
		return n_gram.toString();
	}
	
	public IDFn_gramInfo(){
		this.bookTitle = new Text();
		this.year = new Text();
		this.n_gram = new Text();
		this.decadeIDF = new DoubleWritable();
	}
	
	public IDFn_gramInfo(IDFn_gramInfo rhs){
		this.bookTitle = new Text(rhs.bookTitle);
		this.year = new Text(rhs.year);
		this.n_gram = new Text(rhs.n_gram);
		this.decadeIDF = rhs.decadeIDF;
	}
	
	public IDFn_gramInfo(n_gramInfo info, double idf){
		this.bookTitle = new Text(info.getBookTitle());
		this.year = new Text(info.getYear());
		this.n_gram = new Text(info.get_n_gram());
		decadeIDF = new DoubleWritable(idf);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		bookTitle.readFields(in);
		year.readFields(in);
		n_gram.readFields(in);
		decadeIDF.readFields(in);
		// TODO Auto-generated method stub
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		bookTitle.write(out);
		year.write(out);
		n_gram.write(out);
		decadeIDF.write(out);
	}
	
	@Override
	public String toString(){
		return bookTitle+","+year+","+n_gram+","+decadeIDF;
	}
	
	
}
