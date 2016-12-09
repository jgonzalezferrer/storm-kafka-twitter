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

public class CountBolt extends BaseRichBolt{
	
	// Map to save the list of hashtags occurrences between conditional windows for each language.
		public Map<String, Integer> langMap = new DefaultTreeMap<String, Integer>(0);

		// Key words for conditional windows. The words that initialize conditional windows.
		private String langField;

		// Map to store lapses between conditional windows. We will store words in the hashtags occurrences if this flag is on.	
				
		private int counter = 0;
		private String folder;
		PrintWriter writer;
		
		//Constructor executed in nimbus (central node)
		public CountBolt(String langField, String folder){
			this.langField = langField;
			this.folder=folder;
		}
		
		
		public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
			// Executed in the workers
			// TreeMap to save the words alphabetically.
			
			//Adding keywords: take from inpu
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
		
		private String top3Algorithm(){
			
			int[] top3 = new int[]{0, 0, 0};
			String[] top3k = new String[]{"null", "null", "null"};
			for (Map.Entry<String, Integer> entry : langMap.entrySet()) {
				int value = entry.getValue();				
				String key = entry.getKey();					

				if(value > top3[0]){					
					top3[2] = top3[1];
					top3k[2] = top3k[1];
					top3[1] = top3[0];
					top3k[1] = top3k[0];
					top3[0] = value;
					top3k[0] = key;
				}
				else if(value > top3[1]){
					top3[2] = top3[1];
					top3k[2] = top3k[1];
					top3[1] = value;
					top3k[1] = key;
				}
				else if(value > top3[2]){
					top3[2] = value;
					top3k[2] = key;
				}
			}
			
			String toReturn = ++counter+","+langField+",";
			for(int i=0; i<top3.length; i++){ //Print top 3 keys + its values
				if(i==top3.length-1){
					toReturn += top3k[i]+","+top3[i];
				}
				else{
					toReturn += top3k[i]+","+top3[i]+",";
					
				}
			}
			return toReturn;		
		}
		

		public void execute(Tuple tuple) {
		
			String valueField = (String) tuple.getValueByField("value");
			String endField = (String) tuple.getValueByField("endField");
			
			System.out.println("Hashtag received from Kafka in partial count: "+valueField);
			
			
			//Take keyword from file
			
			if(endField.equals("1")){ //If the window is closed
				writer.write(top3Algorithm());
				writer.flush();
				langMap = new DefaultTreeMap<String, Integer>(0); //Reset window accumulator

			}
				
			else{ //The window is opened
				// Update occurrence
				langMap.put(valueField, langMap.get(valueField)+1);
				
				
				}

		}
		

		public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

		}


}
