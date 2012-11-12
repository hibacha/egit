package edu.neu.cs6240.zhoukang;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import weka.classifiers.Classifier;
import weka.core.Instances;

public class Tools{
    public static final String asciiCharsetName = "us-ascii";

    public static InputStream file2inputStream(String filepath, Configuration conf) throws IOException{
	org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(URI.create(filepath), conf);
	return fs.open(new Path(filepath));
    }

    public static List<String> file2lines(String filepath, Configuration conf) throws IOException{
	InputStream is = file2inputStream(filepath,conf);
	BufferedReader r = new BufferedReader(new InputStreamReader(is,asciiCharsetName));

	List<String> lines = new ArrayList<String>();
	for(String l; (l=r.readLine())!=null; ){ lines.add(l); }
	r.close();
	return lines;
    }
    public static List<String> file2lines_local(File f) throws IOException{
	BufferedReader r = new BufferedReader(new FileReader(f));
	List<String> lines = new ArrayList<String>();
	for(String l; (l=r.readLine())!=null; ){ lines.add(l); }
	r.close();
	return lines;
    }
    public static CharSequence join(CharSequence delim, Collection<?> os) throws IOException{
    	StringBuilder sb = new StringBuilder();
	boolean isFirst = true;
	for(Object o : os){
	    if(isFirst){ isFirst=false; }
	    else{ sb.append(delim); }
	    sb.append(o.toString());
	}
	return sb;
    }
    public static CharSequence join(CharSequence delim, Object[] os) throws IOException{
	StringBuilder sb = new StringBuilder();
	boolean isFirst = true;
	for(Object o : os){
	    if(isFirst){ isFirst=false; }
	    else{ sb.append(delim); }
	    sb.append(o.toString());
	}
	return sb;
    }
    public static boolean[] strList2booleanList(Collection<? extends String> l, String[] tf){
	return strList2booleanList(l.toArray(new String[0]), tf);
    }
    public static boolean[] strList2booleanList(String[] l, String[] tf){
	boolean[] bl = new boolean[l.length];
	for(int i=0; i<l.length; i++){
	    if(l[i].equals(tf[0])){ bl[i]=true; }
	    else if(l[i].equals(tf[1])){ bl[i]=false; }
	    else{ throw new RuntimeException("Illegal value '"+l[i]+"'"); }
	}
	return bl;
    }

    public static FSDataInputStream getFDIS(String filepath, Configuration conf) throws IOException{
	org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(URI.create(filepath), conf);
	FSDataInputStream fdis = fs.open(new Path(filepath));
	return fdis;
    }
    public static List<FSDataInputStream> getFDISList(String[] filepathArray, Configuration conf) throws IOException{
	List<FSDataInputStream> l = new ArrayList<FSDataInputStream>();
	for(int i=0; i<filepathArray.length; i++){ l.add(getFDIS(filepathArray[i], conf)); }
	return l;
    }
    public static List<Double> doubleIterable2list(Iterable<? extends DoubleWritable> i){
	List<Double> l = new ArrayList<Double>();
	for(Iterator<? extends DoubleWritable> it=i.iterator(); it.hasNext();){ l.add(it.next().get()); }
	return l;
    }
    public static double sum(Collection<? extends Double> c){
	double sum=0;
	for(Double d: c){ sum+=d; }
	return sum;
    }

    public static CharSequence[] splitInputStreamByLine(InputStream is, String charsetName, double split_proportion)
	throws UnsupportedEncodingException, IOException{
	BufferedReader reader = new BufferedReader(new InputStreamReader(is, charsetName));
	Random r = new Random(1234);
	StringBuilder[] s = new StringBuilder[]{new StringBuilder(), new StringBuilder()};

	for(String l; (l=reader.readLine())!=null;){
	    if(Pattern.matches("@data\\s*",l)){
		s[0].append(l).append('\n');
		s[1].append(l).append('\n');
	    }
	    else if(r.nextDouble()<split_proportion){ s[0].append(l).append('\n'); }
	    else{ s[1].append(l).append('\n');
	    }
	}
	reader.close();
	return s;
    }
    public static double[] classify(Classifier c, Instances ds) throws Exception{
	double[] r = new double[ds.numInstances()];
	for(int i=0; i<ds.numInstances(); i++){
	    r[i] = c.classifyInstance(ds.instance(i));
	}
	return r;
    }

    public static List<Double> asList(double[] in){
	List<Double> l = new ArrayList<Double>();
	for(double d: in){ l.add(d); }
	return l;
    }

    public static int size(Iterable<?> iterable){
	int size=0;
	for(Iterator<?> it = iterable.iterator(); it.hasNext(); ){
	    size++; it.next();
	}
	return size;
    }

    public static CharSequence iterable2str(Iterable<?> iterable, CharSequence delim){
	StringBuilder sb = new StringBuilder();
	for(Iterator<?> it = iterable.iterator(); it.hasNext(); ){
	    sb.append(it.next().toString()).append(delim);
	}
	//if(sb.length()==0){throw new RuntimeException("Empty string?"); }
	return sb;
    }

    public static class Map extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
	private int count=0;
	@Override
	public void map(LongWritable key, Text value, Context context)
	    throws IOException,InterruptedException{ count++; }
	@Override
	public void cleanup(Context context)
	    throws IOException,InterruptedException{ context.write(new IntWritable(1),new IntWritable(count)); }
    }
    public static class Reduce extends Reducer<IntWritable,IntWritable,NullWritable,IntWritable>{
	private int total=0;
	@Override
	public void reduce(IntWritable key,Iterable<IntWritable> values, Context context)
	    throws IOException,InterruptedException{
	    for(Iterator<IntWritable> it = values.iterator(); it.hasNext();){
		total+=it.next().get();
		//context.write(NullWritable.get(), it.next()); //new IntWritable(total));
	    }
	    context.write(NullWritable.get(), new IntWritable(total));
	}
    }

    public static void main(String[] args) throws Exception{
	//test(args);
	test2(args);
    }
    public static void test(String[] args) throws Exception{
	Configuration conf = new Configuration();
	String[] otherArgs = args; //optionParser.getRemainingArgs();

	Job job = new Job(conf, "runWeka");
	job.setJarByClass(Tools.class);
	job.setMapperClass(Map.class);
	//job.setPartitionerClass(WekaPartition.class);
	//job.setCombinerClass(WekaForest.Combine.class);
	job.setReducerClass(Reduce.class);
	job.setNumReduceTasks(1);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);
	//job.setInputFormatClass(UnsplittableTextInputFormat.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	if(! job.waitForCompletion(true)){ System.exit(1); }

    }

    public static class LCMap extends Mapper<LongWritable,Text,LongWritable,NullWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context)
	    throws IOException,InterruptedException{ context.write(key,NullWritable.get()); }
    }
    public static class LCReduce extends Reducer<LongWritable,NullWritable,LongWritable,NullWritable>{
	@Override
	public void reduce(LongWritable key,Iterable<NullWritable> values, Context context)
	    throws IOException,InterruptedException{ context.write(key,NullWritable.get()); }
    }
    public static void test2(String[] args) throws Exception{
	Configuration conf = new Configuration();

	Job job = new Job(conf, "linecount");
	job.setJarByClass(Tools.class);
	job.setMapperClass(LCMap.class);
	job.setReducerClass(LCReduce.class);
	job.setNumReduceTasks(1);
	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(NullWritable.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	if(! job.waitForCompletion(true)){ System.exit(1); }

    }
}