package org.apache.storm.storm_core;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


public class TwitterScheme implements Scheme {

	private static final long serialVersionUID = 1L;

	public List<Object> deserialize(ByteBuffer bytes) {
		//TODO:Bytes to string, class to get properties from JSON
		//JSONClass json = bytes.toString();
		//String lang = json.getLang();
		//String val = json.getVal();
		String lang = "";
		String val = "";
		return new Values(lang, val);
	}

	public Fields getOutputFields() {
		return new Fields("langs","value");
	}
 
}
