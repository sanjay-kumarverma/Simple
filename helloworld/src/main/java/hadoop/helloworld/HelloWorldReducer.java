package hadoop.helloworld;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HelloWorldReducer extends 	Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		int count=0;
		Iterator<IntWritable> i=value.iterator();
		while(i.hasNext()) {
			IntWritable iw=(IntWritable) i.next();
			count+=iw.get();
		}
		
		context.write(key, new IntWritable(count));
		
	}


	

}
