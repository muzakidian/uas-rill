package durasi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class FilmReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int i,sum =0;
		Text oa = arg0;
		// Disini melakukan reducer yaitu setiap ada film dengan durasi  lebih dari 7200 detik (2 jam) maka
		// sistem akan merecord dan menjumlahkan data
		for(IntWritable data : arg1) {
			i = data.get();
			sum = sum + i;
		}
		IntWritable ob = new IntWritable(sum);
		arg2.write(oa, ob);
	}

}
