package hadoop.custominputformat;

import java.io.IOException;
import java.util.Iterator;

import hadoop.logwritable.datatypes.LogWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogProcessorReducer extends Reducer <Text, IntWritable, Text,IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {

		Iterator<IntWritable> ite = value.iterator();
        int count=0;
		while(ite.hasNext()) {
			IntWritable iw = (IntWritable) ite.next();
			count+=iw.get();
		}
		context.write(new Text(key), new IntWritable(count));
	}


	
	

}
