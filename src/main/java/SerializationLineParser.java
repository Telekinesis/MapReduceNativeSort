import org.telekinesis.commonclasses.entity.Pair;


public interface SerializationLineParser<K, V>
{
    public Pair<K, V> parse(String line);
}
