package io.datamass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

    int columnTotal =0;

    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] words=line.split("\t");

        if (words[0].length()<3) {
            columnTotal=Integer.parseInt(words[1]);
        }
        if (columnTotal!=0) {

            int nrOfOccur = Integer.parseInt(words[1]);
            int percent = nrOfOccur*100/columnTotal;
            String word = words[0] + "-" +nrOfOccur;
            Text outputKey = new Text(word.toUpperCase().trim());
            IntWritable outputValue = new IntWritable(percent);
            con.write(outputKey, outputValue);
        }
    }

}
