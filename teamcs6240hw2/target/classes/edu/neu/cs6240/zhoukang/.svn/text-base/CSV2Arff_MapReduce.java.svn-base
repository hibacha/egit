package edu.neu.cs6240.zhoukang;

import java.io.*;
import java.util.*;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.util.*;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.util.GenericOptionsParser;

import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
 
 
public class CSV2Arff_MapReduce{ // extends Configured implements Tool{
    public static class HeaderMap extends Mapper<LongWritable, Text, IntWritable, BitstringWritable> {
	private StringBuilder sb;
	private int lineCount;
	private BitstringWritable bw = null;
	private static String csvHeaderString;

	private void init(){
	    this.lineCount =0;
	    this.sb = new StringBuilder(csvHeaderString);
	    this.sb.append('\n');
	}
	protected void setup(Context context){
	    Configuration conf = context.getConfiguration();
	    try{ csvHeaderString = Tools.file2lines(conf.get("csv_header"),conf).get(0); }
	    catch(IOException e){ throw new RuntimeException(e); }
	    this.init();
	}

	private void sb2bw() throws IOException{
	    Instances instances = csv_str2Instances(this.sb, Tools.asciiCharsetName);
	    BufferedReader r = instances2reader(instances);
	    List<Boolean> l = arffReader2nominalBooleanList(r);
	    r.close();
	    if(this.bw==null){ this.bw = new BitstringWritable(l); }
	    else{ this.bw.orSelf(l); }

	    this.init();
	}
	
        public void map(LongWritable key, Text value, Context context)
	    throws IOException,InterruptedException {
	    sb.append(value.toString()).append("\n");
	    lineCount++;
	    if(lineCount>=5000){ sb2bw(); } // to avoid OutOfMemoryError
        }
	protected void cleanup(Context context)
	    throws IOException,InterruptedException {
	    if(lineCount>=1){ sb2bw(); }

	    context.write(new IntWritable(1), this.bw); 
	}
    }

    public static class Combine extends Reducer<IntWritable, BitstringWritable, IntWritable, BitstringWritable> {
	public static BitstringWritable combine(Iterable<BitstringWritable> values){
	    BitstringWritable thisBW = null;
	    for(BitstringWritable bw: values){
		if(thisBW==null){ thisBW = new BitstringWritable(bw); }
		else{ thisBW.orSelf(bw); }
	    }
	    return thisBW;
	}
	public void reduce(IntWritable key, Iterable<BitstringWritable> values, Context context)
	    throws IOException,InterruptedException {
	    context.write(key, combine(values));
	}
    }
    
    public static class Reduce extends Reducer<IntWritable, BitstringWritable, NullWritable, Text> {
	public void reduce(IntWritable key, Iterable<BitstringWritable> values, Context context)
	    throws IOException,InterruptedException {
	    BitstringWritable bw = Combine.combine(values);
	    context.write(NullWritable.get(), new Text(bw.toString(dataTypes,"\n").toString()));
	}
    }

    public static final String[] dataTypes = {"nominal","numeric"};

  /**
   * takes 2 arguments:
   * - CSV input file
   * - ARFF output file
   */

    //public static String csvHeaderString;
    public static void main(String[] args) throws Exception {
	if (args.length != 3) {
	    System.err.println("\nUsage: CSV2Arff <header.csv> <data.csv> <OUT_DIR>\n");
	    System.exit(1);
	}

	String OUT_DIR=args[2];
	boolean[] datatypes = getDataTypes(args, OUT_DIR+"/part01-1");
	for(boolean b: datatypes){ System.out.println(b); }
	// Yaofei generates arff
	
    }

    public static void test(String[] args) throws Exception{
	String csvHeaderString = Tools.file2lines_local(new File(args[0])).get(0);
	StringBuilder str = new StringBuilder(csvHeaderString);
	str.append('\n').append( Tools.join("\n",Tools.file2lines_local(new File(args[1]))) );

	Instances instances = csv_str2Instances(str, Tools.asciiCharsetName);
	BufferedReader r = instances2reader(instances);
	List<Boolean> l = arffReader2nominalBooleanList(r);
	r.close();
	BitstringWritable bw = new BitstringWritable(l);

	PrintWriter w = new PrintWriter(new File(args[2]));
	w.println(bw.toString(dataTypes,"\n"));
	w.close();
    }

    public static boolean[] getDataTypes(String[] args, String ofilepath) throws Exception{
	Configuration conf = new Configuration();
	conf.set("csv_header", args[0]);
	//DistributedCache.addCacheFile(new Path(otherArgs[2]).toUri(),conf);

	Job job = new Job(conf, "getDataTypes");
	job.setJarByClass(CSV2Arff_MapReduce.class);
	job.setMapperClass(HeaderMap.class);
	job.setCombinerClass(Combine.class);
	job.setReducerClass(Reduce.class);
	job.setNumReduceTasks(1);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(BitstringWritable.class);
	//job.setInputFormatClass(UnsplittableTextInputFormat.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job, new Path(args[1]));
	FileOutputFormat.setOutputPath(job, new Path(ofilepath));
	if(! job.waitForCompletion(true)){ System.exit(1); }

	return Tools.strList2booleanList(Tools.file2lines(ofilepath,conf), dataTypes);
    }

    public static Instances csv_str2Instances(CharSequence csvCS, String charsetName)
	throws IOException{
	CSVLoader loader = new CSVLoader();
	//loader.setNoHeaderRowPresent(true);
	loader.setSource(new ByteArrayInputStream(csvCS.toString().getBytes( Charset.forName(charsetName) )));
	return loader.getDataSet();
    }

    public static BufferedReader instances2reader(Instances i)
	throws IOException{
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	ArffSaver saver = new ArffSaver();
	saver.setInstances(i);
	//saver.setFile(new File(args[1]));
	saver.setDestination(baos);
	saver.writeBatch();
	return new BufferedReader(new InputStreamReader(new ByteArrayInputStream(baos.toByteArray())));
    }

    public static List<Boolean> arffReader2nominalBooleanList(BufferedReader r)
	throws IOException{
	boolean startedAttribute = false;

	List<Boolean> l = new ArrayList<Boolean>();
	for(String oneline=null; (oneline=r.readLine())!=null; ){
	    oneline = oneline.trim();
	    if(oneline.length()==0){ continue; }
	    String[] tokens = oneline.split("\\s+",3);

	    if(tokens[0].equals("@attribute")){
		startedAttribute = true;
		//System.out.println(tokens[1]);

		if( !(tokens[1].toUpperCase().equals(tokens[1])) ){ l.add(true); } // nominal
		else if( tokens[2].equals("numeric") ){ l.add(false); } // numeric
		else{ l.add(true); } // nominal
	    }
	    else if(startedAttribute){ break; }
	}
	return l;
    }

}
