/**
 * Copyright @Flux7
 */
package com.flux7.bolts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class FileWriterBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private transient OutputCollector collector;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ScoreFetcherBolt.class);

	private transient BufferedWriter fileOneWriter;
	private transient BufferedWriter fileTwoWriter;
	private transient BufferedWriter fileThreeWriter;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {

		File fileOne = new File("fileOne.txt");
		File fileTwo = new File("fileTwo.txt");
		File fileThree = new File("fileThree.txt");

		try {
			if (!fileOne.exists()) {
				fileOne.createNewFile();
			}
			if (!fileTwo.exists()) {
				fileTwo.createNewFile();
			}
			if (!fileThree.exists()) {
				fileThree.createNewFile();
			}

			FileWriter oneWriter = new FileWriter(fileOne.getAbsoluteFile());
			fileOneWriter = new BufferedWriter(oneWriter);

			FileWriter twoWriter = new FileWriter(fileTwo.getAbsoluteFile());
			fileTwoWriter = new BufferedWriter(twoWriter);

			FileWriter threeWriter = new FileWriter(fileThree.getAbsoluteFile());
			fileThreeWriter = new BufferedWriter(threeWriter);

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
			if (score < -2) {
				fileOneWriter.write(score + "\t" + text + "\n");
				fileOneWriter.flush();

			} else if (score < 2) {
				fileTwoWriter.write(score + "\t" + text + "\n");
				fileTwoWriter.flush();

			} else {

				fileThreeWriter.write(score + "\t" + text + "\n");
				fileThreeWriter.flush();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
