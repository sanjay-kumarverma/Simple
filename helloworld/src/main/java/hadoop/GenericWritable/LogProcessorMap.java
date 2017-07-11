package hadoop.GenericWritable;

import java.io.IOException;

import hadoop.GenericWritable.datatype.MultiValueWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogProcessorMap extends Mapper<LongWritable, Text,Text,MultiValueWritable> {
	
	private Text userHostText = new Text();
	private Text requestText = new Text();
	private IntWritable respSize = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
 
		String loggerLine = value.toString();

		String[] lineArray = loggerLine.split(" ");
		if (lineArray.length==10) {
				userHostText.set(new Text(lineArray[0]));
				//lw.setTimeStamp(new Text(lineArray[3]+" "+lineArray[4]));
				requestText.set(new Text(lineArray[5]+" "+lineArray[6]+" "+lineArray[7]));
				// lw.setStatus(new Text(lineArray[8]));
				int respsz = 0;
				try {
					respsz=new Integer(lineArray[9]).intValue(); 
					
				} catch(Exception ex) {
					System.out.println("Response size exception-->"+ex.getMessage());
					respsz=0;
				}
				
				respSize.set(respsz);
				//System.out.println("logger ---->"+lw.getUserIp()+"--"+lw.getTimeStamp()+"--"+lw.getRequestUrl()+"--"+lw.getStatus()+"--"+lw.getResponseSize());
				
				context.write(userHostText,new MultiValueWritable(requestText));
				context.write(userHostText,new MultiValueWritable(respSize));
				
		}
		
		
	}
	
	

}
