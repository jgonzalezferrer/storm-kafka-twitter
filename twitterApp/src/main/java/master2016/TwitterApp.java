package master2016;

import java.io.FileNotFoundException;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.TwitterApi;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.json.*;

/** TwitterApp: reading Tweets from Twitter. 
 * 
 * 
 * Java	application that reads tweets from	both the Twitter Streaming API and preloaded log file.
 * The application uses Apache Kafka to store information in order to be able to run the application in a distributed environment.
 * Furthermore, Kadka is horizontally scalable, fault-tolerant and avoid information lost.
 * 
 * @author: Antonio Javier González Ferrer
 * @author: Aitor Palacios Cuesta
 *  
 */
public class TwitterApp {

	/*
	 * The buffered reader where we will read the tweets from the Twitter API or preloaded log file.
	 */
	private static BufferedReader reader;

	/*
	 * Mode of run: 1 means read from file, 2 read from the Twitter API.
	 */
	private static String mode;

	/*
	 * Key associated with the Twitter App consumer.
	 */
	private static String apiKey;

	/*
	 * Secret associated with the Twitter App consumer.
	 */	
	private static String apiSecret;

	/*
	 * Access token associated with the Twitter App.
	 */
	private static String tokenValue;

	/*
	 * Access token secret.
	 */
	private static String tokenSecret;

	/*
	 *  String in the format IP:port corresponding with the Kafka Broker.
	 */
	private static String kafkaUrl;

	/*
	 * Path to the file with the tweets.
	 */
	private static String fileName;

	public static void main(String[] args){

		// The number of arguments must be exactly 7.
		if (args.length!=7){
			System.err.println("Error in number of parameters");
			return;
		}

		mode = args[0];
		apiKey = args[1];
		apiSecret = args[2];
		tokenValue= args[3];
		tokenSecret = args[4];
		kafkaUrl = args[5];
		fileName = args[6];

		if(mode.equals("1")){ // File mode.
			try{
				reader = new BufferedReader(new FileReader(fileName));
			}
			catch(FileNotFoundException e){
				System.err.println("Unable to read the file with the name "+fileName);
				return;
			}
		}

		else if (mode.equals("2")){ //Read from Twitter App.
			TwitterServiceBuilder twitter = new TwitterServiceBuilder(apiKey, apiSecret, tokenValue, tokenSecret);
			reader = twitter.getReader();			
		}

		else{
			System.err.println("Error in the first parameter, it must be either 1 or 2");
			return;
		}

		// Create the Kafka producer that publishes records to the Kafka cluster.
		TwitterKafkaProducer prod = new TwitterKafkaProducer(kafkaUrl);
		String tweet;
		try {
			while((tweet=reader.readLine())!=null){
				// Receiving JSON format tweet.
				JSONObject obj = new JSONObject(tweet);
				
				// We discard tweets without language field.
				if(!obj.has("lang")) continue;	
				
				String lang = obj.getString("lang");
				JSONArray hashtags = obj.getJSONObject("entities").getJSONArray("hashtags");
				
				// Reading all the hashtags from a teet.
				for(int i=0; i<hashtags.length(); i++){
					// We send every tweet to the cluster in spite of the language, becase Kafka
					// performance is effectively constant with respect to data size.
					String hashtag = hashtags.getJSONObject(i).getString("text");
					prod.sendTweet(new Tweet(lang, hashtag));
				}
			} 	
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
