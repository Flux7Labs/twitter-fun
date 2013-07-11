package com.flux7.tweetsentiment.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TweetSentimentReducer extends Reducer< FloatWritable,Text,FloatWritable,Text>{
  
  private MultipleOutputs<FloatWritable, Text> multipleOutputs;
  private static final Float TWEET_LOWER_BOUNDARY_SCORE = -2.0F;
  
  private static final Float TWEET_UPPER_BOUNDARY_SCORE = 2.0F;
  

  @Override
  protected void setup(Context context){
    multipleOutputs = new MultipleOutputs<FloatWritable, Text>(context);

  }

  @Override
  public void reduce(FloatWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Float score = key.get();
    IntWritable bucket = null;
    int cmp1 = score.compareTo(TWEET_LOWER_BOUNDARY_SCORE);
    if( cmp1 < 0 ){
      bucket = new IntWritable(0);
    }
    else  {
      int cmp2 = score.compareTo(TWEET_UPPER_BOUNDARY_SCORE);
      if( cmp2 >= 0){
        bucket = new IntWritable(2);
      }
      else{
        bucket = new IntWritable(1);
      }
    }
  
    
    for (Text value : values) {
      multipleOutputs.write("text",key, value,  bucket.toString());

    }
    //context.progress();
  }
  
  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    multipleOutputs.close();
   
  }


}
