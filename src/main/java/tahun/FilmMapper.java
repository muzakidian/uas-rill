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
		// index data dipisahkan dengan koma (,) sebagai pemisah
		// Format   : id,title,year,rating,timestamp
		// index 0  : id
		// index 1  : tittle
		// index 2  : year
		// index 3  : rating
		// index 4  : timestamp
		String [] k = data.split("[,]");
		// Disini menggunakan index 2 (tahun) karena akan menghitung jumlah film antara tahun 1950 - 1960
		int year = Integer.parseInt(k[2]);
		if(year >=1950 && year <=1960) {
			Text oa = new Text("Jumlah film antara tahun 1950 - 1960 : ");
			IntWritable ob = new IntWritable(1);
			context.write(oa, ob);
		}
	}

}
