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

public class TwitterApp 
{
	private static BufferedReader reader;

	public static void main( String[] args )
	{
		if (args.length!=7){
			System.err.println("Error in number of parameters");
			return;
		}
		String mode=args[0];
		String apiKey=args[1];
		String apiSecret=args[2];
		String tokenValue=args[3];
		String tokenSecret=args[4];
		String kafkaUrl=args[5];
		String fileName=args[6];

		for(int i=0; i<args.length; i++){
			System.out.println(args[i]);
		}

		if(mode.equals("1")){
			try{
				reader = new BufferedReader(new FileReader(fileName));
			}
			catch(FileNotFoundException e){
				System.err.println("Unable to read the file with the name "+fileName);
				return;
			}
		}
		else if (mode.equals("2")){
			//Read from twitter app
			ServiceBuilder serv = new ServiceBuilder()
					.provider(TwitterApi.class)
					.apiKey(apiKey)
					.apiSecret(apiSecret);

			Token tok = new Token(tokenValue, tokenSecret);

			OAuthRequest request = new OAuthRequest(Verb.GET,"https://stream.twitter.com/1.1/statuses/sample.json");

			serv.build().signRequest(tok, request);
			Response response = request.send();
			reader = new BufferedReader(new InputStreamReader(response.getStream()));

		}
		else{
			System.err.println("Error in the first parameter, it must be either 1 or 2");
			return;
		}
		//TODO: Change topic to the language
		TwitterKafkaProducer prod = new TwitterKafkaProducer(kafkaUrl);
		String tweet;
		try {
			while((tweet=reader.readLine())!=null){
				String line = tweet;
				//System.out.println(line);
				JSONObject obj = new JSONObject(line);
				if(!obj.has("lang")) continue;	
				String lang = obj.getString("lang");
				
				System.out.println("Lang is : "+lang);
				//TODO: if(lang is in listoflangs)
				if (lang.equals("en")){
					JSONArray hashtags = obj.getJSONObject("entities").getJSONArray("hashtags");
					for(int i=0; i<hashtags.length(); i++){
						String hashtag = hashtags.getJSONObject(i).getString("text");
						System.out.println("Hashtag is: "+hashtag);
						prod.sendTweet(new Tweet(lang, hashtag));
					}
				}

			}	







		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
