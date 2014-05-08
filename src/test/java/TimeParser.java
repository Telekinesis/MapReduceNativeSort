

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TimeParser
{
    private static SimpleDateFormat dateFormat = new SimpleDateFormat(
            "MM/dd/yyyy");
    
    public static long parseDateToLong(String dateString)
    {
	try
	{
	    return dateFormat.parse(dateString).getTime();
	}
	catch (ParseException e)
	{
	    e.printStackTrace();
	    throw new TimeParsingException();
	}
    }
}
