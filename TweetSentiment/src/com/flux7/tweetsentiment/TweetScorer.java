package com.flux7.tweetsentiment;

import java.io.*;
import java.util.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class TweetScorer {
  
  private Map<String,Integer> emotionScore = new HashMap<String,Integer>();
  
  public TweetScorer() throws IOException{
    
    //Read the file and store the values as a Map
    
    //TODO : change this path to a configurable one. 
    FileReader reader = new FileReader("AFINN-111.txt");
    BufferedReader br = new BufferedReader(reader);
    String strLine;
    while( (strLine = br.readLine()) != null){
      System.out.println( strLine);
      String[] wordTokens = strLine.split("\t");
      emotionScore.put(wordTokens[0], Integer.parseInt(wordTokens[1]));
    }
    
  }
  
  
  public float getScore( String text){
    float score = 0F;
    String tweetText = null;
    try {
      tweetText = getTweetText( text);
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    if( tweetText != null){
      StringTokenizer tokenizer = new StringTokenizer(tweetText.toLowerCase());
    int emotionSum = 0;
    int numEmotions = 0;
    while(tokenizer.hasMoreTokens() ){
      String word = tokenizer.nextToken();
      // strip off non alpha numeric characters from the word.
      word = word.replaceAll("[^A-Za-z0-9]", "");
      if(emotionScore.containsKey(word) ){
        emotionSum += emotionScore.get(word);
        numEmotions ++;
      }
    }
    if( numEmotions == 0){
      score = 0F;
    }
    else{
      score = (float)emotionSum/numEmotions;
    }
    
    }
    
    return score;
  }
  
  
  public String getTweetText( String text) throws ParseException{
    String tweetText = null;
    JSONParser parser=new JSONParser();
    Object obj = parser.parse(text);
    JSONObject jObj=(JSONObject)obj;
    tweetText = (String)jObj.get("text");
    return tweetText;
  }
  
  

  

}
