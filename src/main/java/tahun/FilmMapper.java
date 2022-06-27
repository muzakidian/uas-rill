package tahun;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilmMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String data = value.toString();
		String [] k = data.split("[,]");
		int year = Integer.parseInt(k[2]);
		if(year >=1950 && year <=1960) {
			Text oa = new Text("Jumlah film antara tahun 1950 - 1960 : ");
			IntWritable ob = new IntWritable(1);
			context.write(oa, ob);
		}
	}

}
