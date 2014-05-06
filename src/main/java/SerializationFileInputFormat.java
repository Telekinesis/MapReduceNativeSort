import java.io.IOException;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class SerializationFileInputFormat<K, V> extends FileInputFormat<K, V>
{
    private final SerializationLineParser<K, V> parser;

    public SerializationFileInputFormat(SerializationLineParser<K, V> parser)
    {
	super();
	this.parser = parser;
    }

    @Override
    public RecordReader<K, V> getRecordReader(InputSplit arg0, JobConf arg1,
	    Reporter arg2) throws IOException
    {
	// TODO Auto-generated method stub
	return null;
    }

}
