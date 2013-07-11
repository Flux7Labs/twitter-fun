package com.flux7.tweetsentiment.mapreduce;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class TweetSentimentDriver extends Configured implements Tool{

  @Override
  public int run(String[] args) throws Exception {
    
    if( args.length <2 ){
      System.err.printf("%s [generic options] <input> <output>\n", 
          getClass().getSimpleName());
      
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    
    Job job = new Job( getConf(),"TweetSentiment");
    job.setJarByClass(getClass());
    
    job.setMapperClass(TweetSentimentMapper.class);
    job.setReducerClass(TweetSentimentReducer.class);
    job.setMapOutputKeyClass(FloatWritable.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class,FloatWritable.class, Text.class);
    
    FileInputFormat.addInputPath(job, new Path( args[ 0]));
    FileOutputFormat.setOutputPath(job, new Path( args[ 1]));
    
    return job.waitForCompletion(true)? 0: 1;
    
    
  }
  
  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    // delete old output
    int exitCode = ToolRunner.run(new TweetSentimentDriver(), args);
    System.exit(exitCode);

  }

}
