import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class SerializationLineRecordReader<K, V> extends RecordReader<K, V>
{
    private static final Log                    logger           = LogFactory
	                                                                 .getLog(FileInputFormat.class);
    private final LineRecordReader              lineRecordReader = new LineRecordReader();
    private final SerializationLineParser<K, V> parser;
    private K                                   currentKey;
    private V                                   currentValue;

    public SerializationLineRecordReader(SerializationLineParser<K, V> parser,
	    Configuration conf)
    {
	this.parser = parser;
    }

    @Override
    public synchronized void close() throws IOException
    {
	lineRecordReader.close();
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException
    {
	return currentKey;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException
    {
	return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
	return lineRecordReader.getProgress();
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
	    throws IOException, InterruptedException
    {
	lineRecordReader.initialize(genericSplit, context);
    }

    @Override
    public synchronized boolean nextKeyValue() throws IOException,
	    InterruptedException
    {
	byte[] line = null;
	if (lineRecordReader.nextKeyValue())
	{
	    Text innerValue = lineRecordReader.getCurrentValue();
	    line = innerValue.getBytes();
	}
	else
	{
	    return false;
	}
	if (line == null) return false;
	String lineString = new String(line);
	KeyValuePair<K, V> serialized = parser.parse(lineString);

	currentKey = serialized.getKey();
	currentValue = serialized.getValue();
	// logger.info("Serialized Data: " + serialized);
	return true;
    }

}
