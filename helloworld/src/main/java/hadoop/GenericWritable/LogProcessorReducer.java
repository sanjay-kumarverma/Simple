package hadoop.GenericWritable;

import java.io.IOException;
import java.util.Iterator;

import hadoop.GenericWritable.datatype.MultiValueWritable;
import hadoop.logwritable.datatypes.LogWritable;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class LogProcessorReducer extends Reducer <Text, MultiValueWritable, Text,Text> {

	private Text result = new Text();
	@Override
	protected void reduce(Text key, Iterable<MultiValueWritable> value, Context context)
			throws IOException, InterruptedException {

		//Iterator<GenericWritable> ite = value.iterator();
        int sum=0;
        StringBuilder requrl=new StringBuilder();
        //Text result = new Text();
		for(MultiValueWritable mw:value) {
			Writable writable = mw.get();
			if (writable instanceof IntWritable )
				sum+=((IntWritable) writable).get();
			else {
				requrl.append(((Text) writable).toString());
			    requrl.append("\t"); }
		}
		result.set(sum+"\t"+requrl);
		context.write(key, result);
	}


	
	

}
