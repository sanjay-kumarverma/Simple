package hadoop.logwritable.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class LogWritable implements WritableComparable<LogWritable> {
	
	private Text userIp;
	private Text timeStamp;
	private Text requestUrl;
	private Text status;
	private IntWritable responseSize;
	
	public LogWritable(){
	       this.userIp=new Text();
	       this.timeStamp=new Text();
	       this.requestUrl=new Text();
	       this.status=new Text();
	       this.responseSize=new IntWritable();		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Data input --->"+in.toString());
		this.userIp.readFields(in);
		this.timeStamp.readFields(in);
		this.requestUrl.readFields(in);
		this.status.readFields(in);
		this.responseSize.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.userIp.write(out);
		this.timeStamp.write(out);
		this.requestUrl.write(out);
		this.status.write(out);
		this.responseSize.write(out);
	}

/*	@Override
	public int compareTo(LogWritable o) {
		// TODO Auto-generated method stub
		if (this.userIp.compareTo(o.getUserIp())==0) {
			return this.timeStamp.compareTo(o.getTimeStamp());
		} else
			
		return this.userIp.compareTo(o.getUserIp());
	}*/
	
	@Override
	public int compareTo(LogWritable o) {
		return this.userIp.compareTo(o.getUserIp());
	}	
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.getUserIp().hashCode();
	}

/*	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if(obj instanceof LogWritable) {
			LogWritable otherLogWritable = (LogWritable)obj;
			return this.userIp.equals(otherLogWritable.getUserIp()) &&
					this.timeStamp.equals(otherLogWritable.getTimeStamp());
		} else
			return false;
		
	}*/
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if(obj instanceof LogWritable) {
			LogWritable otherLogWritable = (LogWritable)obj;
			return this.userIp.equals(otherLogWritable.getUserIp());
		} else
			return false;
		
	}	

	public Text getUserIp() {
		return userIp;
	}

	public void setUserIp(Text userIp) {
		this.userIp = userIp;
	}

	public Text getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Text timeStamp) {
		this.timeStamp = timeStamp;
	}

	public Text getRequestUrl() {
		return requestUrl;
	}

	public void setRequestUrl(Text requestUrl) {
		this.requestUrl = requestUrl;
	}

	public Text getStatus() {
		return status;
	}

	public void setStatus(Text status) {
		this.status = status;
	}

	public IntWritable getResponseSize() {
		return responseSize;
	}

	public void setResponseSize(IntWritable responseSize) {
		this.responseSize = responseSize;
	}	

	
	

}
