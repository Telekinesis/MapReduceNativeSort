import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.TimeParser;

public class DateBasedNativeSorter
{

    private static final Log logger = LogFactory
	                                    .getLog(DateBasedNativeSorter.class);

    public static class AccountParser extends
	    Mapper<LongWritable, Text, AccountKey, Text>
    {
	@Override
	protected void map(
	        LongWritable key,
	        Text value,
	        org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, AccountKey, Text>.Context context)
	        throws IOException, InterruptedException
	{
	    String line = value.toString();
	    String[] terms = line.split(",");
	    if (terms.length == 12)
	    {
		// int field00 = Integer.parseInt(terms[0]);
		// char field01 = terms[1].charAt(0);
		long field02 = TimeParser.parseDateToLong(terms[2]);
		// int field03 = Integer.parseInt(terms[3]);
		// int field04 = Integer.parseInt(terms[4]);
		int field05 = Integer.parseInt(terms[5]);
		int field06 = Integer.parseInt(terms[6]);
		int field07 = Integer.parseInt(terms[7]);
		int field08 = Integer.parseInt(terms[8]);
		// int field09 = Integer.parseInt(terms[9]);
		double new1 = Math.sqrt(1D * field07) / field06;
		AccountKey accountKey = new AccountKey(field02, field05,
		        field06, field08);
		String newString = line + "," + new1;
		context.write(accountKey, new Text(newString));
	    }
	    else
	    {
		logger.info("Malformed record: " + line);
	    }
	}
    }

    public static class AccountPartition extends Partitioner<AccountKey, Text>
    {

	private final TreeMap<Long, Integer> keyMap = new TreeMap<Long, Integer>();

	public AccountPartition()
	{
	    keyMap.put(-1276070400000L, 0);
	    keyMap.put(-1160294400000L, 1);
	    keyMap.put(-1146988800000L, 2);
	    keyMap.put(-999763200000L, 3);
	    keyMap.put(-991468800000L, 4);
	    keyMap.put(-944899200000L, 5);
	    keyMap.put(-923734800000L, 6);
	    keyMap.put(-918979200000L, 7);
	    keyMap.put(-868003200000L, 8);
	    keyMap.put(-818236800000L, 9);
	    keyMap.put(-755510400000L, 10);
	    keyMap.put(-753696000000L, 11);
	    keyMap.put(-716889600000L, 12);
	    keyMap.put(-712310400000L, 13);
	    keyMap.put(-659952000000L, 14);
	    keyMap.put(-657878400000L, 15);
	    keyMap.put(-627984000000L, 16);
	    keyMap.put(-565257600000L, 17);
	    keyMap.put(-502617600000L, 18);
	    keyMap.put(-498297600000L, 19);
	    keyMap.put(-480672000000L, 20);
	    keyMap.put(-470044800000L, 21);
	    keyMap.put(-435052800000L, 22);
	    keyMap.put(-431856000000L, 23);
	    keyMap.put(-366451200000L, 24);
	    keyMap.put(-358588800000L, 25);
	    keyMap.put(-356428800000L, 26);
	    keyMap.put(-312278400000L, 27);
	    keyMap.put(-300700800000L, 28);
	    keyMap.put(-298540800000L, 29);
	    keyMap.put(-292665600000L, 30);
	    keyMap.put(-277459200000L, 31);
	    keyMap.put(-277459200000L, 32);
	    keyMap.put(-263894400000L, 33);
	    keyMap.put(-261648000000L, 34);
	    keyMap.put(-242467200000L, 35);
	    keyMap.put(-235382400000L, 36);
	    keyMap.put(-232617600000L, 37);
	    keyMap.put(-231753600000L, 38);
	    keyMap.put(-216720000000L, 39);
	    keyMap.put(-212313600000L, 40);
	    keyMap.put(-206697600000L, 41);
	    keyMap.put(-202118400000L, 42);
	    keyMap.put(-187948800000L, 43);
	    keyMap.put(-184752000000L, 44);
	    keyMap.put(-161683200000L, 45);
	    keyMap.put(-151488000000L, 46);
	    keyMap.put(-125395200000L, 47);
	    keyMap.put(-115459200000L, 48);
	    keyMap.put(-99388800000L, 49);
	}

	@Override
	public int getPartition(AccountKey key, Text value, int numOfPartitions)
	{
	    long time = key.getField02();
	    try
	    {
		long ceilingKey = keyMap.ceilingKey(time);
		int order = keyMap.get(ceilingKey);
		int partitionID = (int) Math.floor(1D * order / keyMap.size()
		        * numOfPartitions);
		if (partitionID == numOfPartitions) partitionID--;
		return partitionID;
	    }
	    catch (NullPointerException e)
	    {
		logger.warn("Time out of bound " + time);
		return 0;
	    }
	}

    }

    public static class AccountGrouper extends WritableComparator
    {

	protected AccountGrouper()
	{
	    super(AccountKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b)
	{
	    AccountKey key1 = (AccountKey) a;
	    AccountKey key2 = (AccountKey) b;
	    long time1 = key1.getField02();
	    long time2 = key2.getField05();
	    if (time1 > time2)
		return 1;
	    else if (time1 == time2) return 0;
	    return -1;
	}

    }

    public static class AccountReduce extends
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
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args)
	        .getRemainingArgs();
	if (otherArgs.length != 2)
	{
	    System.err.println("Usage: wordcount <in> <out>");
	    System.exit(2);
	}
	Path inputPath = new Path(otherArgs[0]);
	Path outputPath = new Path(otherArgs[1]);

	Job job = new Job(conf);
	job.setJarByClass(DateBasedNativeSorter.class);
	job.setJobName("Date Based Native Sorter");

	job.setMapperClass(AccountParser.class);
	job.setMapOutputKeyClass(AccountKey.class);
	job.setMapOutputValueClass(Text.class);

	job.setReducerClass(AccountReduce.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);

	job.setPartitionerClass(AccountPartition.class);
	job.setGroupingComparatorClass(AccountGrouper.class);

	job.setInputFormatClass(TextInputFormat.class);
	FileInputFormat.setInputPaths(job, inputPath);
	job.setOutputFormatClass(TextOutputFormat.class);
	FileOutputFormat.setOutputPath(job, outputPath);
	outputPath.getFileSystem(conf).delete(outputPath, true);

	// int reducers = Integer.parseInt(otherArgs[2]);
	// job.setNumReduceTasks(reducers);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
