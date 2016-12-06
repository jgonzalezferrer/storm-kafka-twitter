package upm.cloud.project1.twitterApp;
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
import org.scribe.oauth.OAuthService;;

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
        
        if(mode.equals("1")){
        	try{
         reader = new BufferedReader(new FileReader(fileName));}
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
        	reader= new BufferedReader(new InputStreamReader(response.getStream()));
        	
        }
        else{
        	System.err.println("Error in the first parameter, it must be either 1 or 2");
        	return;
        }
        //TODO: Change topic to the language
        TwitterKafkaProducer prod = new TwitterKafkaProducer(kafkaUrl,"myTopic");
        String tweet;
        try {
			while((tweet=reader.readLine())!=null){
				//TODO: Sacar mierdas del tweet (del json y tal)
				prod.sendTweet("myTweet");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
