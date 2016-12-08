package master2016;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


public class TwitterScheme implements Scheme {

	private static final long serialVersionUID = 1L;
	public static String KafkaValue="value";
	
	public List<Object> deserialize(ByteBuffer bytes) {
		//TODO:Bytes to string, class to get properties from JSON
		//JSONClass json = bytes.toString();
		//String lang = json.getLang();
		//String val = json.getVal();
		
		try {
			return new Values(new String(bytes.array(), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return new Values();
		}
	}

	
	public Fields getOutputFields() {
		
		return new Fields("value");
	}
 
}
