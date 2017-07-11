package hadoop.custominputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

public class IPBasedPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text ip, IntWritable value, int numOfReducers) {
		// TODO Auto-generated method stub
		
		String region=getGeoLocation(ip.toString());
		if (region!=null){
			return ((region.hashCode() &
			Integer.MAX_VALUE) % numOfReducers);
			}
		else
			return 0;
		}
	
	
	private String getGeoLocation(String ip) {
		
		LookupService cl=null;
		Location location=null;
		try {
			cl = new LookupService("/home/sanjay/Downloads/GeoLiteCity.dat",
			        LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE);
			 location= cl.getLocation(ip);	

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
		if (location!=null)
		     return location.region;
		else
			return null;
		

	}

}
