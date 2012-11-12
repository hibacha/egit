package edu.neu.cs6240.zhoukang;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
public class TestByte {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
        byte[] b=new byte[]{-121,127,0,90};
        int i=org.apache.hadoop.io.WritableComparator.readVInt(b, 0);
        int size=WritableUtils.decodeVIntSize(b[0]);
        System.out.println(size);
        System.out.println("value is"+ i);
        long ii=1000;
        long a=300;
        float rst=(float)a/(float)ii;
        
        System.out.println(rst);
        Text code=new Text("Carpodacus_mexicanus");
        TextPair p1=new TextPair(code,code);
        TextPair p2=new TextPair(code,new Text("A"));

        System.out.println(code.hashCode());
        WritableComparator com=WritableComparator.get(TextPair.class);
        
       com.compare(new byte[]{121,89,90},0,3,new byte[]{121,34,45},0,3);
	}

}
