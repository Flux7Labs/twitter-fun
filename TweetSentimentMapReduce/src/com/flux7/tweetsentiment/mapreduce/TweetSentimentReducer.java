package com.flux7.tweetsentiment.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TweetSentimentReducer extends Reducer< IntWritable,TweetScoreWrapper,NullWritable,TweetScoreWrapper>{
  
  private MultipleOutputs<NullWritable, TweetScoreWrapper> multipleOutputs;
  
  

  @Override
  protected void setup(Context context){
    multipleOutputs = new MultipleOutputs<NullWritable,TweetScoreWrapper>(context);

  }

  @Override
  public void reduce(IntWritable key, Iterable<TweetScoreWrapper> values, Context context)
      throws IOException, InterruptedException {  
    
    for (TweetScoreWrapper value : values) {
      multipleOutputs.write("text",key, value,  key.toString());

    }
    //context.progress();
  }
  
  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    multipleOutputs.close();
   
  }


}
