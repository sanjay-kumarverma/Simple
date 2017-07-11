package hadoop.custominputformat;

import hadoop.custominputformat.datatypes.LogWritable;

import java.io.IOException;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogProcessorMap extends Mapper<LongWritable, LogWritable,Text,IntWritable> {

	@Override
	protected void map(LongWritable key, LogWritable value,Context context)
			throws IOException, InterruptedException {
 
/*		String loggerLine = value.toString();

		String[] lineArray = loggerLine.split(" ");
		if (lineArray.length==10) {
				LogWritable lw = new LogWritable();
				lw.setUserIp(new Text(lineArray[0]));
				lw.setTimeStamp(new Text(lineArray[3]+" "+lineArray[4]));
				lw.setRequestUrl(new Text(lineArray[5]+" "+lineArray[6]+" "+lineArray[7]));
				lw.setStatus(new Text(lineArray[8]));
				int respsize = 0;
				try {
					respsize=new Integer(lineArray[9]).intValue();  
				} catch(Exception ex) {
					System.out.println("Response size exception-->"+ex.getMessage());
					respsize=0;
				}
				
				lw.setResponseSize(new IntWritable(respsize));
				//System.out.println("logger ---->"+lw.getUserIp()+"--"+lw.getTimeStamp()+"--"+lw.getRequestUrl()+"--"+lw.getStatus()+"--"+lw.getResponseSize());
				
				context.write(lw,new IntWritable(1));
		}
		*/
		context.write(value.getUserIp(),new IntWritable(1));	
		
	}
	
	

}
