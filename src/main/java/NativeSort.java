import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class NativeSort
{
    private static final Log logger = LogFactory.getLog(NativeSort.class);
    
    public static class Map extends Mapper<AccountKey, Text, AccountKey, Text>
    {

	@Override
	protected void map(
	        AccountKey key,
	        Text value,
	        org.apache.hadoop.mapreduce.Mapper<AccountKey, Text, AccountKey, Text>.Context context)
	        throws IOException, InterruptedException
	{
	    context.write(key, value);
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
    
    public static class AccountInputFormat extends SerializationFileInputFormat<AccountKey, Text>{

	public AccountInputFormat()
        {
	    super(new AccountSerializationLineParser());
        }
	
    }

    public static void main(String[] args) throws IOException,
	    ClassNotFoundException, InterruptedException
    {
	if (args.length != 4)
	{
	    logger.error("Must specify paths of input, output, partition file, and the number of reducers");
	    System.exit(-1);
	}
	Path inputPath = new Path(args[0]);
	Path outputPath = new Path(args[1]);
	Path partitionFile = new Path(args[2]);
	int reducers = Integer.parseInt(args[3]);

	Configuration config = new Configuration();
	TotalOrderPartitioner.setPartitionFile(config, partitionFile);

	Job job = new Job(config);
	job.setJarByClass(NativeSort.class);
	job.setJobName("Native Sort");
	
	job.setMapperClass(Map.class);
	job.setMapOutputKeyClass(AccountKey.class);
	job.setMapOutputValueClass(Text.class);
	
	job.setReducerClass(Reduce.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);
	
	job.setPartitionerClass(TotalOrderPartitioner.class);
	
	outputPath.getFileSystem(config).delete(outputPath, true);
	outputPath.getFileSystem(config).delete(partitionFile, true);
	
	job.setInputFormatClass(AccountInputFormat.class);
	FileInputFormat.addInputPath(job, inputPath);
	FileOutputFormat.setOutputPath(job, outputPath);
	
	logger.info("Start writing partition file");
	
	RandomSampler<AccountKey, Text> sampler = new RandomSampler<AccountKey, Text>(0.5, 4, 20);
	InputSampler.writePartitionFile(job, sampler);
	
	logger.info("Partition File written to: " + partitionFile.toUri());
	
	job.setNumReduceTasks(reducers);
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
