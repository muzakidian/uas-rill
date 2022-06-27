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
        // index data dipisahkan dengan koma (,) sebagai pemisah
        // Format   : id,title,year,rating,timestamp
        // index 0  : id
        // index 1  : tittle
        // index 2  : year
        // index 3  : rating
        // index 4  : timestamp
        String [] k = data.split("[,]");
        // Disini menggunakan index 3 (rating) karena akan menghitung jumlah film dengan score 4
        if(!(k[3].equals(""))) {
            double a = (double)Double.valueOf(k[3]);
            if(a > 4.0) {
                Text oa = new Text("Jumlah film yang mempunyai rating  4 (Sempurna): ");
                IntWritable ob = new IntWritable(1);
                // Print data yang telah dipilah
                context.write(oa, ob);
            }
        }
    }
}
