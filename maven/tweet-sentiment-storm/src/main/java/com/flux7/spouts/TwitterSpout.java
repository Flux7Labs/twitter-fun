/**
 * Copyrights @flux7 
 * 
 */
package com.flux7.spouts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.flux7.bolts.ScoreFetcherBolt;
/**
 * 
 * @author rsharif
 *
 */
public class TwitterSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private transient SpoutOutputCollector collector;

	private transient BufferedReader reader;
	
	private String filePath;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterSpout.class);
	
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		File file = new File(filePath);
		try {
			reader = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			LOGGER.error("File with path {} cannot be found ", filePath);
			e.printStackTrace();
		}
		
	}
	
	public TwitterSpout(String filePath){
		this.filePath = filePath;
		
	}
	
	public String getNextLine() {
		try {
			if(reader.ready()){
				return reader.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return "";
	}

	
	public void nextTuple() {
//		try {
//			Thread.currentThread().sleep(1000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		collector.emit(new Values(getNextLine()));
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("words"));
		
	}

}
