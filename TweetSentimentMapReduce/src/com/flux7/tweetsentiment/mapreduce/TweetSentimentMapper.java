package com.flux7.tweetsentiment.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.flux7.tweetsentiment.TweetScorer;

public class TweetSentimentMapper extends

    
    Mapper<LongWritable, Text, FloatWritable,Text>{

  private TweetScorer scorer = new TweetScorer( "AFINN-111.txt");
  
  
  @Override 
  public void map( LongWritable key, Text value, 
            Context context) throws IOException, InterruptedException{
    String text = value.toString();
    String tweetText = scorer.getTweetText(text);
    if( text.length() > 0){
      float tweetScore = scorer.getScore(tweetText);
    context.write(new FloatWritable(tweetScore), new Text(tweetText));
    }
    
  }
  

}
