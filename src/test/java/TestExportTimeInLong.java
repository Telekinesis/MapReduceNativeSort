
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.telekinesis.commonclasses.io.LineParser;
import org.telekinesis.commonclasses.io.LocalLineFileReader;

public class TestExportTimeInLong
{
    private static final String timeFile = "partitionTest.csv";
    
    public static void main(String[] args)
    {
	LocalLineFileReader reader = new LocalLineFileReader();
	TimeLineParser parser = new TimeLineParser();
	reader.read(timeFile, parser, 0);
	List<Long> timeList = parser.getTime();
	Collections.sort(timeList);
	List<TimeKey> keys = new ArrayList<TimeKey>();
	for (int i = 0; i < timeList.size(); i++)
	{
	    keys.add(new TimeKey(timeList.get(i), i));
	}
	for (TimeKey timeKey : keys)
	{
	    System.out.println(String.format("keyMap.put(%dL,%d);",
		    timeKey.getTime(), timeKey.getOrder()));
	}
	TreeMap<Long, Integer> keyMap = new TreeMap<Long, Integer>();
	for (TimeKey timeKey : keys)
	{
	    keyMap.put(timeKey.getTime(), timeKey.getOrder());
	}
	long ceilingKey = keyMap.ceilingKey(-1276070400000L);
	System.out.println(keyMap.get(ceilingKey));
    }

    private static class TimeLineParser implements LineParser
    {
	private final List<Long> timeList = new ArrayList<Long>();

	@Override
	public void parse(String line)
	{
	    long time = TimeParser.parseDateToLong(line);
	    timeList.add(time);
	}

	@Override
	public void onEnd()
	{
	}

	public List<Long> getTime()
	{
	    return timeList;
	}

    }
}
