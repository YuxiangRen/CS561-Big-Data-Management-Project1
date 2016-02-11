package Query3;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;


public class JoinGenericWritable extends GenericWritable {
	
	 private static Class<? extends Writable>[] CLASSES = null;

	    static {
	        CLASSES = (Class<? extends Writable>[]) new Class[] {
	            CustInfo.class,
	            TransInfo.class
	             //add as many different class as you want
	        };
	    }
	
	
	public JoinGenericWritable() {}
	
    public JoinGenericWritable(Writable instance) {
        set(instance);
    }

	@Override
	protected Class<? extends Writable>[] getTypes() {
		// TODO Auto-generated method stub
		return CLASSES;
	}
	
}
