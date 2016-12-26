package master2016;

import java.util.TreeMap;

/** DefaultTreeMap: TreeMap with an instantiated initial value. 
 * 
 * @param <K>, the type of the key.
 * @param <V>, the type of the value.
 * @param defaultValue, the default value to be instantiated in.
 */
public class DefaultTreeMap<K,V> extends TreeMap<K,V> {

	private static final long serialVersionUID = 1L;
	protected V defaultValue;
	public DefaultTreeMap(V defaultValue) {
		this.defaultValue = defaultValue;
	}
	@Override
	public V get(Object k) {
		return containsKey(k) ? super.get(k) : defaultValue;
	}
}
