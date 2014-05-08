import java.io.IOException;

import org.apache.hadoop.io.Text;

import util.TimeParser;

public class AccountSerializationLineParser implements
        SerializationLineParser<AccountKey, Text>
{

    @Override
    public KeyValuePair<AccountKey, Text> parse(String line) throws IOException
    {
	String[] terms = line.split(",");
	// int field00 = Integer.parseInt(terms[0]);
	// char field01 = terms[1].charAt(0);
	long field02 = TimeParser.parseDateToLong(terms[2]);
	// int field03 = Integer.parseInt(terms[3]);
	// int field04 = Integer.parseInt(terms[4]);
	int field05 = Integer.parseInt(terms[5]);
	int field06 = Integer.parseInt(terms[6]);
	int field07 = Integer.parseInt(terms[7]);
	int field08 = Integer.parseInt(terms[8]);
	// int field09 = Integer.parseInt(terms[9]);
	double new1 = Math.sqrt(1D * field07) / field06;
	AccountKey accountKey = new AccountKey(field02, field05, field06,
	        field08);
	String newString = line + "," + new1;
	return new KeyValuePair<AccountKey, Text>(accountKey, new Text(newString));
    }

}
