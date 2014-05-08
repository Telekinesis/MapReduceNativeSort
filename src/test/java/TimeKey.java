


public class TimeKey implements Comparable<TimeKey>
{
    private final long time;
    private final int  order;

    public TimeKey(long time, int order)
    {
	this.time = time;
	this.order = order;
    }

    public long getTime()
    {
	return time;
    }

    public int getOrder()
    {
	return order;
    }

    @Override
    public int compareTo(TimeKey o)
    {
	return Long.compare(time, o.time);
    }

    @Override
    public String toString()
    {
	return "TimeKey [time=" + time + ", order=" + order + "]";
    }
}
