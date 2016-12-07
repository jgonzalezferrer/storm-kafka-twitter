package master2016;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Tweet implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String lang;
	private String hashtag;
	
	public Tweet(String lang, String hashtag){
		this.lang = lang;
		this.hashtag = hashtag;
	}

	public String getLang() {
		return lang;
	}

	public String getHashtag() {
		return hashtag;
	}
	
	public byte[] serialize() throws IOException{
		ByteArrayOutputStream bs= new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream (bs);
		os.writeObject(this); 
		os.close();
		return bs.toByteArray(); // devuelve byte[]
	}
	
	public static Tweet deserialize(byte[] bytes) throws ClassNotFoundException, IOException{
		ByteArrayInputStream bs= new ByteArrayInputStream(bytes); // bytes es el byte[]
		ObjectInputStream is = new ObjectInputStream(bs);
		Tweet tweet = (Tweet)is.readObject();
		is.close();
		return tweet;
		
	}

}
