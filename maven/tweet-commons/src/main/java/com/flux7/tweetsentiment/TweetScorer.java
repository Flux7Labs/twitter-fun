package com.flux7.tweetsentiment;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class TweetScorer {

	private Map<String, Integer> emotionScore = new HashMap<String, Integer>();

	public TweetScorer(String affinFileName)  {

		// Read the file and store the values as a Map

		// TODO : change this path to a configurable one.
		FileReader reader;
		try {
			reader = new FileReader(affinFileName);
			BufferedReader br = new BufferedReader(reader);
			String strLine;
			while ((strLine = br.readLine()) != null) {
				String[] wordTokens = strLine.split("\t");
				emotionScore.put(wordTokens[0], Integer.parseInt(wordTokens[1]));
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

	public float getScore(String tweetText) {
		float score = 0F;
		if (tweetText != null) {
			StringTokenizer tokenizer = new StringTokenizer(
					tweetText.toLowerCase());
			int emotionSum = 0;
			int numEmotions = 0;
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				// strip off non alpha numeric characters from the word.
				word = word.replaceAll("[^A-Za-z0-9]", "");
				if (emotionScore.containsKey(word)) {
					emotionSum += emotionScore.get(word);
					numEmotions++;
				}
			}
			if (numEmotions == 0) {
				score = 0F;
			} else {
				score = (float) emotionSum / numEmotions;
			}

		}

		return score;
	}

	public String getTweetText(String text) throws ParseException {
		String tweetText = null;
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(text);
		JSONObject jObj = (JSONObject) obj;
		tweetText = (String) jObj.get("text");
		return tweetText;
	}

}
