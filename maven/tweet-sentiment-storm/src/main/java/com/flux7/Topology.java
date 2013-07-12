/**
 * Copyright 2013 flux7.com 
 */
package com.flux7;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.flux7.bolts.ScoreFetcherBolt;
import com.flux7.bolts.filewriters.WriterOne;
import com.flux7.bolts.filewriters.WriterThree;
import com.flux7.bolts.filewriters.WriterTwo;
import com.flux7.spouts.TwitterSpout;

/**
 * 
 * @author rsharif
 *
 */
public class Topology {
	
	private static final String WRITER_THREE = "writerThree";
	private static final String WRITER_TWO = "writerTwo";
	private static final String WRITER_ONE = "writerOne";
	private static final String SCORER = "scorer";
	private static final String WORDS = "words";
	private static final Logger LOGGER = LoggerFactory.getLogger(Topology.class);
	
	public static void main(String [] args){
		
		if(args.length < 2){
			LOGGER.error("USAGE : java -jar /path/to/twitter/file /path/to/AFFIN-file",args.length);
			return;
		}
		
		String twitterSampleFile = args[0];
		String affinFile= args[1];
		
		TopologyBuilder builder = new TopologyBuilder(); 
		
		builder.setSpout(WORDS, new TwitterSpout(twitterSampleFile), 1);        
		
		builder.setBolt(SCORER, new ScoreFetcherBolt(affinFile), 5)
		        .shuffleGrouping(WORDS);
		
		builder.setBolt(WRITER_ONE, new WriterOne(), 1)
        .shuffleGrouping(SCORER,ScoreFetcherBolt.LESS_THAN_NEGATIVE_2);
		
		builder.setBolt(WRITER_TWO, new WriterTwo(), 1)
        .shuffleGrouping(SCORER,ScoreFetcherBolt.LESS_THAN_TWO);
		
		builder.setBolt(WRITER_THREE, new WriterThree(), 1)
        .shuffleGrouping(SCORER,ScoreFetcherBolt.GREATER_THAN_TWO);
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("twitter", conf, builder.createTopology());
		
		LOGGER.debug("Starting Topology twitter");
		
	}
}
