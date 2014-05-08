import java.io.IOException;


public interface SerializationLineParser<K, V>
{
    public KeyValuePair<K, V> parse(String line) throws IOException;
}
