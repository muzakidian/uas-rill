package durasi;

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
		if(k.length == 5) {
		if(!(k[4].equals(""))) {
			int i = Integer.parseInt(k[4]);
			if(i > 7200) {
			Text oa = new Text("Jumlah film yang mempunyai durasi lebih dari 2 jam : ");
			IntWritable ob = new IntWritable(1);
			context.write(oa, ob);
			}
		}
	}
	}

}
