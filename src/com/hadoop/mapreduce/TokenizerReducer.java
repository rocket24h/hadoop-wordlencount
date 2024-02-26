package com.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TokenizerReducer extends Reducer<Text, IntWritable, Text, Text>{
  public void reduce(Text word, Iterable<IntWritable> word_length, Context con) throws IOException, InterruptedException{
    Text word_type = new Text();
    word_type.set(check_length(word.getLength()));
    con.write(word, word_type);
  }

  public String check_length(Integer word_length) {
    if (word_length >= 10) {
        return "Big";
    }
    else if (word_length > 5) {
        return "Medium";
    }
    else if (word_length > 2) {
        return "Small";
    }
    return "Tiny";
  }

}