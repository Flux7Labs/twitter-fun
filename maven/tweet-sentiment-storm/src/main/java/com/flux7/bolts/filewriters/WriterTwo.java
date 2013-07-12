/**
 * Copyright @Flux7
 */
package com.flux7.bolts.filewriters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flux7.bolts.ScoreFetcherBolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * 
 * @author rsharif
 * 
 */
public class WriterTwo extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private transient OutputCollector collector;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ScoreFetcherBolt.class);

	private transient BufferedWriter fileWriter;

	private static String FILE_NAME = "fileTwo.txt";

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {

		File file = new File(FILE_NAME);

		try {
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter writer = new FileWriter(file.getAbsoluteFile());
			fileWriter = new BufferedWriter(writer);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void execute(Tuple input) {
		float score = input.getFloat(0);
		String text = input.getString(1);
		LOGGER.debug("score {} for text {}", score, text);
		persistScore(score, text);

	}

	private void persistScore(float score, String text) {
		try {
			fileWriter.write(score + "\t" + text + "\n");
			fileWriter.flush();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
