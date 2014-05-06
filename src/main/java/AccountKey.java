
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class AccountKey implements WritableComparable<AccountKey>
{
    private LongWritable field02;
    private IntWritable  field05;
    private IntWritable  field06;
    private IntWritable  field08;

    public AccountKey()
    {
	field02 = new LongWritable();
	field05 = new IntWritable();
	field06 = new IntWritable();
	field08 = new IntWritable();
    }

    public AccountKey(long field02, int field05, int field06, int field08)
    {
	this.field02 = new LongWritable(field02);
	this.field05 = new IntWritable(field05);
	this.field06 = new IntWritable(field06);
	this.field08 = new IntWritable(field08);
    }

    @Override
    public void readFields(DataInput input) throws IOException
    {
	field02.readFields(input);
	field05.readFields(input);
	field06.readFields(input);
	field08.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException
    {
	field02.write(output);
	field05.write(output);
	field06.write(output);
	field08.write(output);
    }

    public long getField02()
    {
	return field02.get();
    }

    public int getField05()
    {
	return field05.get();
    }

    public int getField06()
    {
	return field06.get();
    }

    public int getField08()
    {
	return field08.get();
    }

    @Override
    public int compareTo(AccountKey o)
    {
	int cmp = field02.compareTo(o.field02);
	if (cmp != 0) return cmp;
	cmp = field05.compareTo(o.field05);
	if (cmp != 0) return -cmp;
	cmp = field06.compareTo(o.field06);
	if (cmp != 0) return -cmp;
	cmp = field08.compareTo(o.field08);
	return cmp;
    }

    @Override
    public String toString()
    {
	return "AccountKey [field02=" + field02 + ", field05=" + field05
	        + ", field06=" + field06 + ", field08=" + field08 + "]";
    }

}
