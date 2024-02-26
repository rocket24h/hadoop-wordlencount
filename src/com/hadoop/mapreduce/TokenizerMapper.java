package com.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {


    public void map(Object key, Text value, Context con) throws IOException, InterruptedException {
        String content = value.toString();
        // StringTokenizer itr = new StringTokenizer(content);
        IntWritable len = new IntWritable();
        // Text word = new Text();
        // while (itr.hasMoreTokens())
        // {
        //     word.set(itr.nextToken());
        //     len.set(word.getLength());
        //     con.write(word, len);
        // }
        String[] words = content.split(" ");
        for (String word: words)
        {
            String trimmed_word = word.trim();
            len.set(trimmed_word.length());
            Text reduceKey = new Text(trimmed_word);
            con.write(reduceKey, new IntWritable(1));
        }
    }
    
}
