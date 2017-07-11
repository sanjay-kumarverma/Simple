package hadoop.custominputformat;

import java.io.IOException;

import hadoop.custominputformat.datatypes.LogWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class LogFileRecordReader extends RecordReader<LongWritable, LogWritable> {
	
	LineRecordReader lineReader;
	LogWritable value;

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		lineReader.close();
		
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return lineReader.getCurrentKey();
	}

	@Override
	public LogWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lineReader.getProgress();
	}

	@Override
	public void initialize(InputSplit inputsplit, TaskAttemptContext attempt)
			throws IOException, InterruptedException {
		lineReader = new LineRecordReader();
		lineReader.initialize(inputsplit, attempt);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
        
		
		if(!lineReader.nextKeyValue())
			return false;
		
		String line=lineReader.getCurrentValue().toString();
		
		String[] lineArray = line.split(" ");
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
				
				value=lw;
		}		
		
		return true;
	}

}
