import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class SerializationFileInputFormat<K, V> extends FileInputFormat<K, V>
{
    private final SerializationLineParser<K, V> parser;

    public SerializationFileInputFormat(SerializationLineParser<K, V> parser)
    {
	this.parser = parser;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file)
    {
	final CompressionCodec codec = new CompressionCodecFactory(
	        context.getConfiguration()).getCodec(file);
	return codec == null;
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit genericSplit,
	    TaskAttemptContext context) throws IOException,
	    InterruptedException
    {
	context.setStatus(genericSplit.toString());
	return new SerializationLineRecordReader<K, V>(parser,
	        context.getConfiguration());
    }

}
