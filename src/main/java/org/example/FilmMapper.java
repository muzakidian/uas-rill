package org.example;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
public class FilmMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        String data = value.toString();
        String [] k = data.split("[,]");
        if(!(k[3].equals(""))) {
            double a = (double)Double.valueOf(k[3]);
            if(a > 4.0) {
                Text oa = new Text("Jumlah film yang mempunyai rating lebih dari 4 : ");
                IntWritable ob = new IntWritable(1);
                context.write(oa, ob);
            }
        }
    }
}
