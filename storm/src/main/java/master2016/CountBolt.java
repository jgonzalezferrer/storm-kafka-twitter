package master2016;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/** CountBolt: calculating the Top 3 trending topic for a given language.
 * 
 * Given  a  set  of  languages,  the  Top3  algorithm  calculates  the  three  most  used hashtags for a given language 
 * in between two occurrences of a given hashtag (defined by the conditional window).
 * 
 * In case of tie, order the top alphabetically.
 * If in a given window the list is not complete, the outpul will be "null" as value and 0 for the counter.
 * 
 * @param: langField, language for the conditional window.
 * @param: folder, folder where the output will be written down.
 *
 */
public class CountBolt extends BaseRichBolt{	

	private static final long serialVersionUID = -3520089917050763992L;

	/*
	 * Tree map to save the list of hashtags occurrences between conditional windows.
	 * This data structure has been chosen because it is a map structure that stores alphabetically ordered the elements by key.
	 * This will be useful in case of ties in the rank.
	 * 
	 * DefaultTreeMap implements a TreeMap but it is initialized to a default value.
	 */
	public Map<String, Integer> langMap = new DefaultTreeMap<String, Integer>(0);

	/*
	 * Counter to enumerate the conditional windows.
	 */
	private int counter = 0;

	/*
	 * Language for the given conditional window.
	 */
	private String langField;

	/*
	 * Folder where the output will be written down.
	 */
	private String folder;

	/*
	 * PrintWrite object to write in a file.
	 */
	private PrintWriter writer;

	/*
	 * Constructor executed in nimbus (central node).
	 */
	public CountBolt(String langField, String folder){
		this.langField=langField;
		this.folder=folder;
	}

	/*
	 * Method executed in the workers that provides the bolt with an output collector used to emit tuples from this bolt.
	 *
	 * @param: conf, the Storm configuration for this spout.
	 * @param: context, information about the task within the topology.
	 * @param: collector, used to emit tuples from this spout.
	 */
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {			
		try {
			this.writer = new PrintWriter(this.folder+"/"+this.langField+"_01.log", "UTF-8");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * This method calculates and write the three most used hashtag given a language in between two occurrences of
	 * the conditional window. We have decided to implement this own method in to make the sort more efficient. 
	 * 
	 * After a conditional window ends, the hashtags are ordered alphabetically. Then, the next step is to sort them by value, maintaining the 
	 * previous alphabetically order. Instead of sorting again by value (which complexity would take n*log(n)) we iterate over the whole list, 
	 * updating the highest Top3 values. The complexity of this method is just linear (n). This will lead to the expected solution, since it is not
	 * necessary to sort all the elements in order to get the Top 3 elements.
	 * 
	 */
	private String top3Algorithm(){
		// By default, the keys are 'null' and the values '0'.
		int[] top3 = new int[]{0, 0, 0};
		String[] top3k = new String[]{"null", "null", "null"};

		// Iterating over all the elements, which are alphabetically ordered.
		for (Map.Entry<String, Integer> entry : langMap.entrySet()) {
			int value = entry.getValue();				
			String key = entry.getKey();					

			// New top 1.
			if(value > top3[0]){					
				top3[2] = top3[1];
				top3k[2] = top3k[1];
				top3[1] = top3[0];
				top3k[1] = top3k[0];
				top3[0] = value;
				top3k[0] = key;
			}
			// New top 2.
			else if(value > top3[1]){
				top3[2] = top3[1];
				top3k[2] = top3k[1];
				top3[1] = value;
				top3k[1] = key;
			}

			// New top 3.
			else if(value > top3[2]){
				top3[2] = value;
				top3k[2] = key;
			}
		}

		// Generating the expected output.
		String toReturn = ++counter+","+langField+",";
		for(int i=0; i<top3.length; i++){ //Print to file top 3 keys + its values
			if(i==top3.length-1){
				toReturn += top3k[i]+","+top3[i]+"\n";
			}
			else{
				toReturn += top3k[i]+","+top3[i]+",";

			}
		}
		return toReturn;		
	}


	/*
	 * Process a single tuple of input.
	 * 
	 * @param tuple, tuple to process.
	 */
	public void execute(Tuple tuple) {
		String valueField = (String) tuple.getValueByField(ConditionalWindowBolt.HASHTAGFIELD);
		String endField = (String) tuple.getValueByField(ConditionalWindowBolt.STOPCONDITIONFIELD);


		//If the window is closed, we write into the file.
		if(endField.equals("1")){ 
			writer.write(top3Algorithm());
			writer.flush();
			langMap = new DefaultTreeMap<String, Integer>(0); //Reset window accumulator
		}

		else{ //The window is opened
			// Update occurrence
			langMap.put(valueField, langMap.get(valueField)+1);
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}
}
