package cs455.mapreduce.TFIDF.decadeIDF;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class n_gramInfo implements Writable{
	Text bookTitle;
	Text year;
	Text n_gram;
	
	public n_gramInfo(){
		this.bookTitle = new Text();
		this.year = new Text();
		this.n_gram = new Text();
	}
	
	public n_gramInfo(n_gramInfo rhs){
		this.bookTitle = new Text(rhs.bookTitle);
		this.year = new Text(rhs.year);
		this.n_gram = new Text(rhs.n_gram);
	}
	
	public n_gramInfo(String bookTitle, String year, String n_gram){
		this.bookTitle = new Text(bookTitle);
		this.year = new Text(year);
		this.n_gram = new Text(n_gram);
	}
	
	public String get_n_gram(){
		return n_gram.toString();
	}
	
	public String getYear(){
		return year.toString();
	}
	
	public String getBookTitle(){
		return bookTitle.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		bookTitle.readFields(in);
		year.readFields(in);
		n_gram.readFields(in);
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		bookTitle.write(out);
		year.write(out);
		n_gram.write(out);
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public String toString(){
		return bookTitle+","+year+","+n_gram;
	}

}
