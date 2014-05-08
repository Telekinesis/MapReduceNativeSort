import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.util.ReflectionUtils;


public class TestWritePartitionFile
{
    private static final Log logger = LogFactory.getLog(TestWritePartitionFile.class);
    
    public static void main(String[] args) throws IOException, InterruptedException
    {
	Configuration conf = new Configuration();
	Job job = new Job(conf);
	job.setMapOutputKeyClass(AccountKey.class);
	int numPartitions = 4;
	job.setNumReduceTasks(numPartitions);
	
	
	final Path partFile = new Path("hdfs://bigant-dev-001:9000/qyj/nativesort/partition");
	final FileSystem fs = partFile.getFileSystem(conf);
	fs.initialize(partFile.toUri(), conf);
	
	
	final InputFormat<AccountKey, Text> inf = new SerializationFileInputFormat<AccountKey, Text>(new AccountSerializationLineParser());
	RandomSampler<AccountKey, Text> sampler = new RandomSampler<AccountKey, Text>(0.5, 4, 20);
	AccountKey[] samples = sampler.getSample(inf, job);
//	    final InputFormat inf = 
//	        ReflectionUtils.newInstance(job.getInputFormatClass(), conf);
//	    int numPartitions = job.getNumReduceTasks();
//	    K[] samples = sampler.getSample(inf, job);
//	    LOG.info("Using " + samples.length + " samples");
//	    RawComparator<K> comparator =
//	      (RawComparator<K>) job.getSortComparator();
//	    Arrays.sort(samples, comparator);
//	    Path dst = new Path(TotalOrderPartitioner.getPartitionFile(conf));
//	    FileSystem fs = dst.getFileSystem(conf);
//	    if (fs.exists(dst)) {
//	      fs.delete(dst, false);
//	    }
//	    SequenceFile.Writer writer = SequenceFile.createWriter(fs, 
//	      conf, dst, job.getMapOutputKeyClass(), NullWritable.class);
//	    NullWritable nullValue = NullWritable.get();
//	    float stepSize = samples.length / (float) numPartitions;
//	    int last = -1;
//	    for(int i = 1; i < numPartitions; ++i) {
//	      int k = Math.round(stepSize * i);
//	      while (last >= k && comparator.compare(samples[last], samples[k]) == 0) {
//	        ++k;
//	      }
//	      writer.append(samples[k], nullValue);
//	      last = k;
//	    }
//	    writer.close();
    }
}
