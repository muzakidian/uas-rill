package org.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App {
    public static void main(String arg[])throws Exception{
        Configuration conf = new Configuration();
        //Menentukan job
        Job job = new Job(conf);
        job.setJarByClass(App.class);
        // Menentukan Map Class
        job.setMapperClass(FilmMapper.class);
        // Menentukan Reduce Class
        job.setReducerClass(FilmReducer.class);
        //Menentukan [MAP] Output key dan Value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //Menentukan path input dan output
        Path input = new Path(arg[0]);
        Path output = new Path(arg[1]);
        // Menentukan class arguments
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        //Menentukan class format input dan output
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // Submit job lalu melakukan proses pemilihan sampai job selesai
        job.waitForCompletion(true);
    }
}
