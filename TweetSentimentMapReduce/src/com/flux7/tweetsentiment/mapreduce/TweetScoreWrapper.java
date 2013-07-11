package com.flux7.tweetsentiment.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TweetScoreWrapper implements WritableComparable<TweetScoreWrapper> {
  
  private Text tweetText;
  private FloatWritable score;

  public TweetScoreWrapper(){
    this.tweetText = new Text();
    this.score = new FloatWritable();
  }
  
  public TweetScoreWrapper( String text, Float score){
    this.tweetText = new Text( text);
    this.score = new FloatWritable(score);
  }
  
  public TweetScoreWrapper( Text text, FloatWritable score){
    this.score  = score;
    this.tweetText = text;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.score.readFields(in);
    this.tweetText.readFields(in);
    
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.score.write(out);
    this.tweetText.write(out);
    
  }

  @Override
  public int compareTo(TweetScoreWrapper o) {
    int result;
    result = this.score.compareTo(o.getScore());
    if( result == 0){
      result = this.tweetText.compareTo(o.getTweetText());
    }
    
    return result;
  }

  @Override
  public String toString(){
    return ( this.score + "\t" + this.tweetText);
  }
  public Text getTweetText() {
    return tweetText;
  }

  public void setTweetText(Text tweetText) {
    this.tweetText = tweetText;
  }

  public FloatWritable getScore() {
    return score;
  }

  public void setScore(FloatWritable score) {
    this.score = score;
  }

}
