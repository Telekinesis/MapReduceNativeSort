

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Account implements Writable
{
    private int    field00;
    private char   field01;
    private long   field02;
    private int    field03;
    private int    field04;
    private int    field05;
    private int    field06;
    private int    field07;
    private int    field08;
    private int    field09;
    private double new1;
    
    public Account()
    {
    }

    public Account(int field00, char field01, long field02, int field03,
	    int field04, int field05, int field06, int field07, int field08,
	    int field09, double new1)
    {
	this.field00 = field00;
	this.field01 = field01;
	this.field02 = field02;
	this.field03 = field03;
	this.field04 = field04;
	this.field05 = field05;
	this.field06 = field06;
	this.field07 = field07;
	this.field08 = field08;
	this.field09 = field09;
	this.new1 = new1;
    }

    @Override
    public void readFields(DataInput input) throws IOException
    {
	field00 = input.readInt();
	field01 = input.readChar();
	field02 = input.readLong();
	field03 = input.readInt();
	field04 = input.readInt();
	field05 = input.readInt();
	field06 = input.readInt();
	field07 = input.readInt();
	field08 = input.readInt();
	field09 = input.readInt();
	new1 = input.readDouble();
    }

    @Override
    public void write(DataOutput output) throws IOException
    {
	output.write(field00);
	output.write(field01);
	output.writeLong(field02);
	output.write(field03);
	output.write(field04);
	output.write(field05);
	output.write(field06);
	output.write(field07);
	output.write(field08);
	output.write(field09);
	output.writeDouble(new1);
    }

}
