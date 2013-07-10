package com.flux7.tweetsentiment;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.gson.Gson;

public class TweetScorer extends Mapper<LongWritable,Text,Text,FloatWritable>{
  
  private Map<String,Integer> emotionScore = new HashMap<String,Integer>();
  
  public TweetScorer() throws IOException{
    super();
    //Read the file and store the values as a Map
    
    
    FileReader reader = new FileReader("/host/linux_workspace/twitter-fun/tweet-sentiment/AFINN-111.txt");
    BufferedReader br = new BufferedReader(reader);
    String strLine;
    while( (strLine = br.readLine()) != null){
      String[] wordTokens = strLine.split(" ");
      emotionScore.put(wordTokens[0], Integer.parseInt(wordTokens[1]));
    }
    
  }
  
  @Override
  public void map(LongWritable key, Text value, 
            Context context) throws IOException, InterruptedException {
    
    //
    Gson gson = new Gson();
    String text = gson.fromJson(value.toString(), String.class);
  }
  
  private float getScore( String text){
    float score = 0;
    //String[] rawWords = text.toLowerCase().split(" ");
    StringTokenizer tokenizer = new StringTokenizer(text.toLowerCase());
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
    
    return score;
  }
  
  
  

}
