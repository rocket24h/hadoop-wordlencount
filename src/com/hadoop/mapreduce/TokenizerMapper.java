package com.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
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

    public void map(Object key, Text value, Context con) throws IOException, InterruptedException {
        String content = value.toString();
        String[] words = content.split(" ");
        for (String word: words)
        {
            String trimmed_word = word.trim();
            String word_length_type = check_length(trimmed_word.length());
            Text reduceKey = new Text(trimmed_word);
            Text reduceValue = new Text(word_length_type);
            con.write(reduceKey, reduceValue);
        }
    }
    
}
