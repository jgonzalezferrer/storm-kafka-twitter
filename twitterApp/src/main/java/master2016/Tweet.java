package master2016;

import java.io.Serializable;

/** 
 * Tweet: storing the language and the hashtag of a Tweet.
 * @param: lang, language of the tweet.
 * @param hashtag, hashtag of the tweet.
 */
public class Tweet implements Serializable{

	private static final long serialVersionUID = 4395192960007693104L;
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

}
