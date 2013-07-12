package com.flux7;



  import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.json.simple.parser.ParseException;

import com.flux7.tweetsentiment.TweetScorer;

public class TweetSentimentBasic {
  private static final Float TWEET_LOWER_BOUNDARY_SCORE = -2.0F;

  private static final Float TWEET_UPPER_BOUNDARY_SCORE = 2.0F;

  public void getTweetScores(String[] args) {

    String wordScoreFile = args[1];
    String outputPath = args[2];

    try {
      FileReader ttReader = new FileReader(args[0]);
      TweetScorer ts = new TweetScorer(args[1]);

      BufferedReader br = new BufferedReader(ttReader);
      File path = new File(args[2]);
      if (!path.exists()) {

        path.mkdir();
      }
      FileWriter f1 = new FileWriter(args[2] + "/score-0.txt");
      FileWriter f2 = new FileWriter(args[2] + "/score-1.txt");
      FileWriter f3 = new FileWriter(args[2] + "/score-2.txt");

      BufferedWriter bw1 = new BufferedWriter(f1);
      BufferedWriter bw2 = new BufferedWriter(f1);
      BufferedWriter bw3 = new BufferedWriter(f1);

      String line;
      while ((line = br.readLine()) != null) {

        String tweetText = ts.getTweetText(line);
        if (tweetText != null) {
          Float score = ts.getScore(tweetText);
          System.out.println( score + "\t" + tweetText);
          int cmp1 = score.compareTo(TWEET_LOWER_BOUNDARY_SCORE);
          if (cmp1 < 0) {
            bw1.write(score + "\t" + tweetText);
          } else {
            int cmp2 = score.compareTo(TWEET_UPPER_BOUNDARY_SCORE);
            if (cmp2 >= 0) {
              bw3.write(score + "\t" + tweetText);
            } else if( (cmp1 >= 0) && (cmp2 < 0 )){
              bw2.write(score + "\t" + tweetText);
            }
          }
        }

      }
      
      bw1.close();
      bw2.close();
      bw3.close();
      br.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("Please provide the valid paths for the input files");
      System.exit(-1);
    }

    TweetSentimentBasic tsBasic = new TweetSentimentBasic();
    tsBasic.getTweetScores(args);
  }


}
