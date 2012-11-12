package edu.neu.cs6240.zhoukang;

import java.util.*;
import java.io.*;
import org.apache.hadoop.io.WritableComparable;

public class BitstringWritable implements WritableComparable<BitstringWritable>{
    private byte[] l;
    private int n;

    public BitstringWritable(){ super(); }
    public BitstringWritable(boolean[] v){ super(); this.set(v); }
    public BitstringWritable(Collection<? extends Boolean> v){ super(); this.set(v); }
    public BitstringWritable(DataInput in) throws IOException{ super(); this.readFields(in); }
    public BitstringWritable(BitstringWritable bw){ super(); this.n=bw.size(); this.l = Arrays.copyOf(bw.l,bw.size());   }

    public void orBit(int i, boolean b){ this.l[i/8] |= ((b?1:0)<<i%8); }
    public boolean getBit(int i){ return (l[i/8] & (1<<i%8))==0?false:true; }
    public int size(){ return this.n; }
    public int getByteN(){ return n/8+(n%8==0?0:1); }
    public void set(boolean[] in){
	this.n = in.length;
	this.l = new byte[this.getByteN()];
	Arrays.fill(this.l, (byte)0);
	for(int i=0; i<n; i++){ this.orBit(i,in[i]); }
    }
    public void set(Collection<? extends Boolean> in){
	this.n = in.size();
	this.l = new byte[this.getByteN()];
	Arrays.fill(this.l, (byte)0);
	Iterator<? extends Boolean> it=in.iterator();
	for(int i=0; it.hasNext(); i++){ this.orBit(i,it.next()); }
    }
    public boolean[] toBooleanArray(){
	boolean[] bList = new boolean[n];
	for(int i=0; i<n; i++){ bList[i] = this.getBit(i); }
	return bList;
    }

    @Override
	public int compareTo(BitstringWritable bw){
	int min = Math.min(this.size(),bw.size());
	for(int i=0; i<min; i++){
	    int c = (this.getBit(i)?1:0)-(bw.getBit(i)?1:0);
	    if(c!=0){ return c; }
	}
	return this.size()-bw.size();
    }

    public void orSelf(boolean[] bl){
	if(this.size()!=bl.length){ throw new RuntimeException("Size of bitstring different!"); }
	for(int i=0; i<this.size(); i++){ this.orBit(i, bl[i]); }
    }
    public void orSelf(Collection<? extends Boolean> bl){
	if(this.size()!=bl.size()){ throw new RuntimeException("Size of bitstring different!"); }
	Iterator<? extends Boolean> it = bl.iterator();
	for(int i=0; it.hasNext(); i++){ this.orBit(i, it.next()); }
    }
    public void orSelf(BitstringWritable bw){
	if(this.size()!=bw.size()){ throw new RuntimeException("Size of bitstring different!"); }
	for(int i=0; i<this.size(); i++){ this.orBit(i, bw.getBit(i)); }
    }

    @Override
	public void write(DataOutput out) throws IOException {
	out.writeInt(n);
	out.write(this.l);
    }

    @Override
	public void readFields(DataInput in) throws IOException {
	this.n = in.readInt();
	l = new byte[this.getByteN()];
	in.readFully(l, 0, this.getByteN());
    }

    public CharSequence toString(String[] vs, String delim){
	if(vs.length!=2){ throw new RuntimeException("Exactly 2 values required!"); }
	StringBuilder sb = new StringBuilder();
	for(int i=0; i<this.size(); i++){
	    if(i!=0){ sb.append(delim); }
	    sb.append(this.getBit(i)?vs[0]:vs[1]);
	}
	return sb;
    }

    public static void main(String[] args) throws Exception{
	test01();
    }
    private static void test01() throws Exception{
	boolean[] b1 = {true,false,true,true,false,true,true,false,true,true,false};
	BitstringWritable bw1 = new BitstringWritable(b1);

	File f = new File("/tmp/t");
	DataOutputStream os = new DataOutputStream(new FileOutputStream(f));
	bw1.write(os);
	os.close();

	DataInputStream is = new DataInputStream(new FileInputStream(f));
	BitstringWritable bw2 = new BitstringWritable(is);
	is.close();

	boolean[] b2 = bw2.toBooleanArray();
	System.out.println(b1.length + " vs " +b2.length);
	for(int i=0; i<b1.length; i++){
	    System.out.println(b1[i] + " " +b2[i]);
	}
    }
}