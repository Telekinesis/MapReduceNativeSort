
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedPartitioner;

public class NativeSort
{
    public static class Map extends
	    Mapper<LongWritable, Text, AccountKey, Text>
    {
	private static SimpleDateFormat dateFormat = new SimpleDateFormat(
	                                                   "MM/dd/yyyy");

	@Override
	protected void map(
	        LongWritable key,
	        Text value,
	        org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, AccountKey, Text>.Context context)
	        throws IOException, InterruptedException
	{
	    String line = value.toString();
	    String[] terms = line.split(",");
	    // int field00 = Integer.parseInt(terms[0]);
	    // char field01 = terms[1].charAt(0);
	    long field02 = parseDateToLong(terms[2]);
	    // int field03 = Integer.parseInt(terms[3]);
	    // int field04 = Integer.parseInt(terms[4]);
	    int field05 = Integer.parseInt(terms[5]);
	    int field06 = Integer.parseInt(terms[6]);
	    int field07 = Integer.parseInt(terms[7]);
	    int field08 = Integer.parseInt(terms[8]);
	    // int field09 = Integer.parseInt(terms[9]);
	    double new1 = Math.sqrt(1D * field07) / field06;
	    AccountKey accountKey = new AccountKey(field02, field05, field06,
		    field08);
	    String newString = line + "," + new1;
	    context.write(accountKey, new Text(newString));
	}

	private long parseDateToLong(String dateString) throws IOException
	{
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
    
    public static class Partition extends Partitioner<AccountKey, Text>{
	
	@Override
        public int getPartition(AccountKey key, Text value, int numOfReducers)
        {
	    // TODO Auto-generated method stub
	    return 0;
        }
	
    }

    public static class Reduce extends
	    Reducer<AccountKey, Text, Text, NullWritable>
    {
	@Override
	protected void reduce(
	        AccountKey key,
	        Iterable<Text> values,
	        org.apache.hadoop.mapreduce.Reducer<AccountKey, Text, Text, NullWritable>.Context context)
	        throws IOException, InterruptedException
	{
	    for (Text account : values)
	    {
		context.write(account, NullWritable.get());
	    }
	}
    }

    public static void main(String[] args) throws IOException,
	    ClassNotFoundException, InterruptedException
    {
	if (args.length != 4)
	{
	    System.err.println("Must specify input and output paths");
	    System.exit(-1);
	}
	Path inputPath = new Path(args[0]);
	Path outputPath = new Path(args[1]);
	Path partitionFile = new Path(args[2]);
	int reducers =  Integer.parseInt(args[3]);
	
	KeyFieldBasedPartitioner<AccountKey, Text> partitioner = new KeyFieldBasedPartitioner<AccountKey, Text>();
	
	Job job = new Job();
	job.setJarByClass(NativeSort.class);
	job.setJobName("Native Sort");

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);

	job.setMapOutputKeyClass(AccountKey.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);
	job.setNumReduceTasks(Integer.parseInt(args[2]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
