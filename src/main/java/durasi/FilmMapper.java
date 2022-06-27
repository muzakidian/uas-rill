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
		// index data dipisahkan dengan koma (,) sebagai pemisah
		// Format   : id,title,year,rating,timestamp
		// index 0  : id
		// index 1  : tittle
		// index 2  : year
		// index 3  : rating
		// index 4  : timestamp
		String [] k = data.split("[,]");
		if(k.length == 5) {
		// Disini menggunakan index 4 (durasi) karena akan menghitung jumlah film yang mempunyai durasi lebih dari 2 jam
		if(!(k[4].equals(""))) {
			int i = Integer.parseInt(k[4]);
			//2 jam = 7200 detik
			if(i > 7200) {
			Text oa = new Text("Jumlah film yang mempunyai durasi lebih dari 2 jam : ");
			IntWritable ob = new IntWritable(1);
			context.write(oa, ob);
			}
		}
	}
	}

}
