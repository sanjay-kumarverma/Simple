package hadoop.GenericWritable.datatype;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MultiValueWritable extends GenericWritable {

	//private static Class[] CLASSES = new Class[] { IntWritable.class, Text.class };
	
    private static Class<? extends Writable>[] CLASSES = null;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[] {
            org.apache.hadoop.io.IntWritable.class,
            org.apache.hadoop.io.Text.class
             //add as many different class as you want
        };
    }	
	
	@Override
	protected Class<? extends Writable>[] getTypes() {
		// TODO Auto-generated method stub
		return CLASSES;
	}

	public MultiValueWritable() {
		
	}
	
	public MultiValueWritable(Writable value) {
		set(value);
	}

	
}
