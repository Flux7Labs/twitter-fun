package com.flux7;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

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
      Scanner sc = new Scanner( new File(args[0]));
      File path = new File(args[2]);
      if (!path.exists()) {

        path.mkdir();
      }
      
      
      
      PrintWriter p1 = new PrintWriter( new BufferedWriter( new FileWriter(args[2] + "/score-0.txt")));
      PrintWriter p2 = new PrintWriter( new BufferedWriter( new FileWriter(args[2] + "/score-1.txt")));
      PrintWriter p3 = new PrintWriter( new BufferedWriter( new FileWriter(args[2] + "/score-2.txt")));

      String line;
      while (sc.hasNextLine()) {
        line = sc.nextLine();
        System.out.println( "Line: " + line);
        if (line.length() > 0) {
          String tweetText = ts.getTweetText(line);
          if (tweetText != null) {
            Float score = ts.getScore(tweetText);
            System.out.println(score + "\t" + tweetText);
            int cmp1 = score.compareTo(TWEET_LOWER_BOUNDARY_SCORE);
            if (cmp1 < 0) {
              p1.println(score + "\t" + tweetText + "\n");
              
            } else {
              int cmp2 = score.compareTo(TWEET_UPPER_BOUNDARY_SCORE);
              if (cmp2 >= 0) {
                p3.println(score + "\t" + tweetText + "\n");
              } else if ((cmp1 >= 0) && (cmp2 < 0) ) {
                p2.println(score + "\t" + tweetText + "\n");
              }
            }
          }

        }

      }

      p1.close();
      p2.close();
      p3.close();
      br.close();
      sc.close();
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
