package com.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TonkenizerReducer extends Reducer<Text, Text, Text, Text>{
    public void reduce(Text word, Text word_type, Context con) throws IOException, InterruptedException {
        con.write(word, word_type);
    } 
}
