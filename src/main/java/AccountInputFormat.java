import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class AccountInputFormat extends FileInputFormat<AccountKey, Text>
{
    private 
    @Override
    public RecordReader<AccountKey, Text> createRecordReader(InputSplit arg0,
            TaskAttemptContext arg1) throws IOException, InterruptedException
    {
	// TODO Auto-generated method stub
	return new RecordReader<AccountKey, Text>()
	{

	    @Override
            public void close() throws IOException
            {
	        // TODO Auto-generated method stub
	        
            }

	    @Override
            public AccountKey getCurrentKey() throws IOException,
                    InterruptedException
            {
	        // TODO Auto-generated method stub
	        return null;
            }

	    @Override
            public Text getCurrentValue() throws IOException,
                    InterruptedException
            {
	        // TODO Auto-generated method stub
	        return null;
            }

	    @Override
            public float getProgress() throws IOException, InterruptedException
            {
	        // TODO Auto-generated method stub
	        return 0;
            }

	    @Override
            public void initialize(InputSplit arg0, TaskAttemptContext arg1)
                    throws IOException, InterruptedException
            {
	        // TODO Auto-generated method stub
	        
            }

	    @Override
            public boolean nextKeyValue() throws IOException,
                    InterruptedException
            {
	        // TODO Auto-generated method stub
	        return false;
            }
	};
    }


}
