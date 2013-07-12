package com.flux7.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.json.simple.parser.ParseException;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.flux7.tweetsentiment.TweetScorer;

public class TweetSentimentMapper extends

Mapper<LongWritable, Text, IntWritable, TweetScoreWrapper> {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(TweetSentimentMapper.class);

  private TweetScorer scorer;
  private static final Float TWEET_LOWER_BOUNDARY_SCORE = -2.0F;

  private static final Float TWEET_UPPER_BOUNDARY_SCORE = 2.0F;
  
  @Override
  protected void setup( Context context){
    
    
      Configuration conf = new Configuration();
      String path = conf.get("hadoop.tmp.dir");
      System.out.println( path);
      
    
    scorer = new TweetScorer(path +"/AFINN-111.txt");
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String text = value.toString();

    if (text.length() > 0) {
      String tweetText;
      try {
        tweetText = scorer.getTweetText(text);
        float tweetScore = scorer.getScore(tweetText);
        IntWritable bucket = null;
        Float score = new Float(tweetScore);
        int cmp1 = score.compareTo(TWEET_LOWER_BOUNDARY_SCORE);
        if (cmp1 < 0) {
          bucket = new IntWritable(0);
        } else {
          int cmp2 = score.compareTo(TWEET_UPPER_BOUNDARY_SCORE);
          if (cmp2 >= 0) {
            bucket = new IntWritable(2);
          } else {
            bucket = new IntWritable(1);
          }
        }
        if( tweetText != null){
          TweetScoreWrapper tweetScoreText = new TweetScoreWrapper(tweetText, tweetScore);
        context.write(bucket, tweetScoreText);
        }
        else{
          Log.warn( "TweetText is null!!!!!");
        }
        
      } catch (ParseException e) {
        LOGGER.error("Exception", e);
      }

    }

  }

}
