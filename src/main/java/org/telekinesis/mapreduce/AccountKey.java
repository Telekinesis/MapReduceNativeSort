package org.telekinesis.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class AccountKey implements WritableComparable<AccountKey>
{
    private long   field02;
    private int    field05;
    private int    field06;
    private int    field08;
    
    public AccountKey(long field02, int field05, int field06, int field08)
    {
	this.field02 = field02;
	this.field05 = field05;
	this.field06 = field06;
	this.field08 = field08;
    }

    @Override
    public void readFields(DataInput input) throws IOException
    {
	field02 = input.readLong();
	field05 = input.readInt();
	field06 = input.readInt();
	field08 = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException
    {
	output.writeLong(field02);
	output.write(field05);
	output.write(field06);
	output.write(field08);
    }

    @Override
    public int compareTo(AccountKey o)
    {
	int cmp = Long.compare(field02, o.field02);
	if(cmp != 0)
	    return cmp;
	cmp = Integer.compare(field05, o.field05);
	if(cmp != 0)
	    return -cmp;
	cmp = Integer.compare(field06, o.field06);
	if(cmp != 0)
	    return -cmp;
	cmp = Integer.compare(field08, o.field08);
	return cmp;
    }
}
