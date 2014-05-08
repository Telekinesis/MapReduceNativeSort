import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;

public class TestFindPartitionFile
{
    public static final String DEFAULT_PATH     = "_partition.lst";
    public static final String PARTITIONER_PATH = "mapreduce.totalorderpartitioner.path";
    private static final Log logger = LogFactory.getLog(TestFindPartitionFile.class);

    public static void main(String[] args) throws IOException
    {
	Configuration conf = new Configuration();
	Job job = new Job(conf);
	job.setMapOutputKeyClass(AccountKey.class);
	
	final Path partFile = new Path("hdfs://bigant-dev-001:9000/qyj/nativesort/partition");
	final FileSystem fs = partFile.getFileSystem(conf);
	fs.initialize(partFile.toUri(), conf);
	logger.info(fs.toString());
	AccountKey[] splitPoints = readPartitions(fs, partFile,
	        AccountKey.class, conf);
	logger.info("Start exporting accountKeys");
	logger.info("Split Point count: " + splitPoints.length);
	for (AccountKey accountKey : splitPoints)
        {
	    logger.info(accountKey.toString());
        }
	// if (splitPoints.length != job.getNumReduceTasks() - 1) {
	// throw new IOException("Wrong number of partitions in keyset");
	// }
	// RawComparator<K> comparator =
	// (RawComparator<K>) job.getSortComparator();
	// for (int i = 0; i < splitPoints.length - 1; ++i) {
	// if (comparator.compare(splitPoints[i], splitPoints[i+1]) >= 0) {
	// throw new IOException("Split points are out of order");
	// }
	// }
	// boolean natOrder =
	// conf.getBoolean(NATURAL_ORDER, true);
	// if (natOrder && BinaryComparable.class.isAssignableFrom(keyClass)) {
	// partitions = buildTrie((BinaryComparable[])splitPoints, 0,
	// splitPoints.length, new byte[0],
	// // Now that blocks of identical splitless trie nodes are
	// // represented reentrantly, and we develop a leaf for any trie
	// // node with only one split point, the only reason for a depth
	// // limit is to refute stack overflow or bloat in the pathological
	// // case where the split points are long and mostly look like bytes
	// // iii...iixii...iii . Therefore, we make the default depth
	// // limit large but not huge.
	// conf.getInt(MAX_TRIE_DEPTH, 200));
	// } else {
	// partitions = new BinarySearchNode(splitPoints, comparator);
	// }
	// } catch (IOException e) {
	// throw new IllegalArgumentException("Can't read partitions file", e);
	// }
    }

    private static AccountKey[] readPartitions(FileSystem fs, Path p,
	    Class<AccountKey> keyClass, Configuration conf) throws IOException
    {
	SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
	ArrayList<AccountKey> parts = new ArrayList<AccountKey>();
	AccountKey key = ReflectionUtils.newInstance(keyClass, conf);
	NullWritable value = NullWritable.get();
	try
	{
	    while (reader.next(key, value))
	    {
		logger.info("Inside reader");
		parts.add(key);
		key = ReflectionUtils.newInstance(keyClass, conf);
	    }
	    reader.close();
	    reader = null;
	}
	finally
	{
	    if(reader != null)
		reader.close();
	}
	return parts.toArray((AccountKey[]) Array.newInstance(keyClass, parts.size()));
    }
}
