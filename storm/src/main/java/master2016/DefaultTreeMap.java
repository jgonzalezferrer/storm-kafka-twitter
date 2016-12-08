package master2016;

import java.util.TreeMap;

public class DefaultTreeMap<K,V> extends TreeMap<K,V> {
	  protected V defaultValue;
	  public DefaultTreeMap(V defaultValue) {
	    this.defaultValue = defaultValue;
	  }
	  @Override
	  public V get(Object k) {
	    return containsKey(k) ? super.get(k) : defaultValue;
	  }
	}
