/**
 * Copyright 2013 flux7.com 
 */
package com.flux7;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.flux7.bolts.FileWriterBolt;
import com.flux7.bolts.ScoreFetcherBolt;
import com.flux7.spouts.TwitterSpout;

/**
 * 
 * @author rsharif
 *
 */
public class Topology {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Topology.class);
	
	public static void main(String [] args){
		
		if(args.length < 2){
			LOGGER.error("USAGE : java -jar /path/to/twitter/file /path/to/AFFIN-file",args.length);
			return;
		}
		
		String twitterSampleFile = args[0];
		String affinFile= args[1];
		
		TopologyBuilder builder = new TopologyBuilder(); 
		
		builder.setSpout("words", new TwitterSpout(twitterSampleFile), 1);        
		builder.setBolt("scorer", new ScoreFetcherBolt(affinFile), 1)
		        .shuffleGrouping("words");
		builder.setBolt("filewriter", new FileWriterBolt(), 1)
        .shuffleGrouping("scorer");
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("twitter", conf, builder.createTopology());
		
		LOGGER.debug("Starting Topology twitter");
		
	}
}
