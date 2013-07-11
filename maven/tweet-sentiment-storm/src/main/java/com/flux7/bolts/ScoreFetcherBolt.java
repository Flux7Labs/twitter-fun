/**
 * Copyright@Flux7
 */
package com.flux7.bolts;

import java.util.Map;

import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.flux7.tweetsentiment.TweetScorer;
/**
 * 
 * @author rsharif
 *
 */
public class ScoreFetcherBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private transient OutputCollector collector;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ScoreFetcherBolt.class);
	
	private transient TweetScorer scorer;

	private String affinFileName;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.scorer = new TweetScorer(affinFileName);
		
	}
	public ScoreFetcherBolt(String affinFileName){
		this.affinFileName = affinFileName;
	}
	public void execute(Tuple input) {
		String tweet  = input.getString(0);

		if(tweet.length() != 0){
			try {
				String tweetText = scorer.getTweetText(tweet);
				float score = scorer.getScore(tweetText);
				collector.emit(new Values(score,tweetText));
			} catch (ParseException e) {
				LOGGER.error("Failed to parse tweet {}",tweet);
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("score","tweet_text"));
		
	}

}
