
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

    @Override
    public int hashCode()
    {
	final int prime = 31;
	int result = 1;
	result = prime * result
	        + ( ( field02 == null ) ? 0 : field02.hashCode() );
	result = prime * result
	        + ( ( field05 == null ) ? 0 : field05.hashCode() );
	result = prime * result
	        + ( ( field06 == null ) ? 0 : field06.hashCode() );
	result = prime * result
	        + ( ( field08 == null ) ? 0 : field08.hashCode() );
	return result;
    }

    @Override
    public boolean equals(Object obj)
    {
	if (this == obj) return true;
	if (obj == null) return false;
	if (getClass() != obj.getClass()) return false;
	AccountKey other = (AccountKey) obj;
	if (field02 == null)
	{
	    if (other.field02 != null) return false;
	}
	else if (!field02.equals(other.field02)) return false;
	if (field05 == null)
	{
	    if (other.field05 != null) return false;
	}
	else if (!field05.equals(other.field05)) return false;
	if (field06 == null)
	{
	    if (other.field06 != null) return false;
	}
	else if (!field06.equals(other.field06)) return false;
	if (field08 == null)
	{
	    if (other.field08 != null) return false;
	}
	else if (!field08.equals(other.field08)) return false;
	return true;
    }

}
