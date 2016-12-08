package master2016;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.storm.kafka.StringScheme;
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
		
		return new Values(StringScheme.deserializeString(bytes));
	}

	
	public Fields getOutputFields() {
		
		return new Fields("value");
	}
 
}
