package org.telekinesis.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.xml.soap.Text;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class NativeSort
{
    public static class Map extends Mapper<LongWritable, Text, AccountKey, Account>{
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
	@Override
	protected void map(LongWritable key, Text value,
	        org.apache.hadoop.mapreduce.Mapper.Context context)
	        throws IOException, InterruptedException
	{
	    String[] terms = value.toString().split(",");
	    int field00 = Integer.parseInt(terms[0]);
	    int field01 = terms[1].charAt(0);
	    long field02 = parseDateToLong(terms[2]);
	    int field03 = Integer.parseInt(terms[0]);
	    int field04 = Integer.parseInt(terms[0]);
	    int field05 = Integer.parseInt(terms[0]);
	    int field06 = Integer.parseInt(terms[0]);
	    int field07 = Integer.parseInt(terms[0]);
	    int field08 = Integer.parseInt(terms[0]);
	    int field09 = Integer.parseInt(terms[0]);
	    double new1 = Integer.parseInt(terms[0]);
	    super.map(key, value, context);
	}
	
	private long parseDateToLong(String dateString) throws IOException{
	    try
            {
	        return dateFormat.parse(dateString).getTime();
            }
            catch (ParseException e)
            {
	        e.printStackTrace();
	        throw new IOException(e);
            }
	}
    }
}
