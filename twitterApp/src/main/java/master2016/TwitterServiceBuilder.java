package master2016;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.TwitterApi;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;

/** TwitterServiceBuilder: Connecting to the Twitter Streaming API.
 * 
 * This class connects to the Twitter Streaming API using the OAuth protocol. 
 * This protocol provides authorized access to the API in a secure and standard way.
 * 
 * @param: apiKey, key associated with the Twitter App consumer.
 * @param apiSecret, secret associated with the Twitter App consumer.
 * @param tokenValue, access token associated with the Twitter App.
 * @param: tokenSecret, access token secret.
 * @return: a Tweet buffered reader to retrieve tweets.
 *
 */
public class TwitterServiceBuilder {

	private BufferedReader reader;
	private final String TWITTERSTREAM = "https://stream.twitter.com/1.1/statuses/sample.json";

	public TwitterServiceBuilder(String apiKey, String apiSecret, String tokenValue, String tokenSecret){
		ServiceBuilder serv = new ServiceBuilder()
				.provider(TwitterApi.class)
				.apiKey(apiKey)
				.apiSecret(apiSecret);

		Token tok = new Token(tokenValue, tokenSecret);

		OAuthRequest request = new OAuthRequest(Verb.GET, TWITTERSTREAM);

		serv.build().signRequest(tok, request);
		Response response = request.send();
		this.reader = new BufferedReader(new InputStreamReader(response.getStream()));
	}
	
	public BufferedReader getReader(){
		return this.reader;
	}
	

}
