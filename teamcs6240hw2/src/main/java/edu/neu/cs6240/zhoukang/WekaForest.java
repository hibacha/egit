package edu.neu.cs6240.zhoukang;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import weka.classifiers.Classifier;
import weka.classifiers.trees.RandomTree;
import weka.core.converters.ConverterUtils.DataSource;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.supervised.instance.Resample;

public class WekaForest{
    public static final String classAttributeName = "Agelaius_phoeniceus";

    public static class WekaPartition extends Partitioner<LongWritable, Text> {
	public int getPartition(LongWritable key, Text value, int numPartitions) {
	    return (int)(key.get() % numPartitions);
	}
    }

    public static class WekaMap extends Mapper<LongWritable, Text, LongWritable, Text> {

	private Random testR;
	private Random[] rs;
	private int N, treesCount;
	private long M;
	private double probN, probX;

	//private java.util.Map<Long,CharSequence> = new HashMap<Long,CharSequence>();

	protected void setup(Context context){
	    Configuration conf = context.getConfiguration();

	    this.treesCount = Integer.parseInt(conf.get("treesCount"));
	    if(this.treesCount==0){ throw new RuntimeException("Invalid treesCount "+treesCount+"\n"); }
	    testR = new Random(1000);
	    rs = new Random[treesCount];
	    for(int i=0; i<this.treesCount; i++){ rs[i] = new Random(1000+i); }

	    int X = Integer.parseInt(conf.get("X"));
	    this.M = Long.parseLong(conf.get("M"));
	    this.N = Integer.parseInt(conf.get("N"));
	    this.probX = ((double)X)/this.M;
	    this.probN = ((double)N)/(this.M-X);
	}

	public void map(LongWritable key, Text value, Context context)
	    throws IOException,InterruptedException {
	    //if(key.get()>52598075){ throw new RuntimeException("Number of instances wrong!!!!\n"+key.get()+" vs "+this.M+"\n"); }
	    if(testR.nextDouble()<this.probX){ // test data
		for(int i=0; i<this.treesCount; i++){
		    context.write(new LongWritable( (key.get()+1)*this.treesCount+i ), value);
		}
	    }else{ // train data
		for(int i=0; i<this.treesCount; i++){
		    double d = rs[i].nextDouble();
		    if(d<this.probN){ context.write(new LongWritable(i), value); }
		}
	    }
        }
    }
    public static class WekaReduce extends Reducer<LongWritable, Text, LongWritable, Text> {

	//private List<CharSequence> list = new ArrayList<CharSequence>();
	private int treesCount;
	private String headerPath;
	private java.util.Map<Long,Classifier> classifiers = new HashMap<Long,Classifier>();
	private java.util.Map<Long,Boolean> predictionsDone = new HashMap<Long,Boolean>();
	private long testCount=0;

	protected void setup(Context context){
	    Configuration conf = context.getConfiguration();
	    this.treesCount = Integer.parseInt(conf.get("treesCount"));
	    this.headerPath = conf.get("arff_header");
	}

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
	    throws IOException,InterruptedException {
	    long k = key.get();

	    Configuration conf = context.getConfiguration();

	    //StringBuilder dataSB = new StringBuilder("");
	    //CharSequencedataSB.append(
	    //System.err.println("================= Size: "+Tools.size(values));
	    Instances data;
	    {
		InputStream headerIS = Tools.getFDIS(this.headerPath,conf);
		String content = Tools.iterable2str(values,"\n").toString();
		InputStream dataIS = new ByteArrayInputStream(content.getBytes(Tools.asciiCharsetName));
		InputStream is = new SequenceInputStream(headerIS, dataIS);
		try{
		    data = DataSource.read(is);
		    if(data==null){ throw new Exception(); }
		    //if(Tools.size(values)<=1 && k<this.treesCount){ throw new RuntimeException("Single instance!!!\nk="+k); }
		}
		catch(Exception e){
		    System.err.println("============ error while reading data (key:"+k+") ============");
		    System.err.println(this.headerPath);
		    System.err.println(content);
		    throw new RuntimeException(e);
		}
		is.close();
		content = null; // to free memory
	    }

	    data.setClass(data.attribute(classAttributeName));

	    if(k<this.treesCount){// train comes all together
		Resample rs = new Resample();
		rs.setNoReplacement(false);
		rs.setRandomSeed(1234);

		Instances resampledData;
		try{
		    rs.setInputFormat(data);
		    resampledData = Filter.useFilter(data, rs);
		}catch(Exception e){ throw new RuntimeException(e); }
		resampledData.setClass(resampledData.attribute(classAttributeName));
		data = null; // to free memory

		//System.out.println("======== Training: "+k);
		if(this.classifiers.containsKey(k)){ throw new RuntimeException("Classifier already trained!"); }
		Classifier c = new RandomTree();
		System.gc();
		try{ c.buildClassifier(resampledData); }
		catch(Exception e){ throw new RuntimeException(e); }
		this.classifiers.put(k,c);
	    }else{ // eval
		this.testCount++;
		//System.out.println("======== Predicting: "+k);
		if(this.predictionsDone.containsKey(k/this.treesCount)){ throw new RuntimeException("Prediction should be done only once!"); }
		double[] predictions;
		try{ predictions = Tools.classify(this.classifiers.get(k%this.treesCount),data); }
		catch(Exception e){ throw new RuntimeException(e); }

		Attribute predictionAttribute = data.attribute(classAttributeName);
		if(predictions.length != data.numInstances()){ throw new RuntimeException("Impossible!"); }
		for(int i=0; i<predictions.length; i++){
		    Instance instance = data.instance(i);
		    double trueValue = instance.value(predictionAttribute);
		    //String returnText = predictions[i]+","+trueValue;
		    context.write(new LongWritable(k/this.treesCount), new Text(predictions[i]+","+trueValue));
		}
		this.predictionsDone.put(k,true);
	    }
	}
	protected void cleanup(Context context)
	    throws IOException,InterruptedException{
	    System.err.println("testCount="+this.testCount);
	}
    }

    private static void runWeka(String[] args) throws Exception{ //indir, odir, M, N, X, treesCount
	//int treesCount = 200;

	Configuration conf = new Configuration();
	conf.set("arff_header", args[0]);
	//for(int i=2;i<=9;i++){ conf.set("arff_data"+i, args[0]+"/part-r-0000"+i); }

	conf.set("M", args[3]);
	conf.set("N", args[4]);
	conf.set("X", args[5]);
	String treesCount = args[6];
	conf.set("treesCount", treesCount);
	//conf.set("numReduceTask", Integer.toString(numReduceTask));

	Job job = new Job(conf, "runWeka");
	job.setJarByClass(WekaForest.class);
	job.setMapperClass(WekaMap.class);
	job.setPartitionerClass(WekaPartition.class);
	//job.setCombinerClass(WekaForest.Combine.class);
	job.setReducerClass(WekaReduce.class);
	job.setNumReduceTasks(Integer.parseInt(treesCount));
	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(Text.class);
	//job.setInputFormatClass(UnsplittableTextInputFormat.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job, new Path(args[1]));
	FileOutputFormat.setOutputPath(job, new Path(args[2]+"/step01"));
	if(! job.waitForCompletion(true)){ System.exit(1); }
    }


    public static class OldvgMap extends Mapper<LongWritable, Text, LongWritable, Text> {
	public void map(LongWritable key, Text value, Context context)
	    throws IOException,InterruptedException {
	    String[] F = value.toString().trim().split("\\s+");
	    if(F.length!=2){ throw new RuntimeException(""); }
	    context.write(new LongWritable( Long.parseLong(F[0]) ), new Text(F[1]));
        }
    }
    public static class AvgMap extends Mapper<LongWritable, Text, LongWritable, Text> {
	public void map(LongWritable key, Text value, Context context)
	    throws IOException,InterruptedException {
	    String[] F = value.toString().trim().split("\\s+");
	    if(F.length!=2){ throw new RuntimeException(""); }
	    context.write(key, new Text(F[1]));
        }
    }
    public static class AvgReduce extends Reducer<LongWritable, Text, NullWritable, Text> {
	private int treesCount;
	public void setup(Context context){
	    this.treesCount = Integer.parseInt(context.getConfiguration().get("treesCount"));
	}
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
	    throws IOException,InterruptedException {
	    //List<Double> l = Tools.doubleIterable2list(values);
	    //if(l.size()!=treesCount){ throw new RuntimeException(l.size()+" vs "+treesCount); }

	    int count = 0;
	    double sum = 0;
	    Double trueValue = null;
	    for(Iterator<Text> it = values.iterator(); it.hasNext(); ){
		String v = it.next().toString();
		String[] tokens = v.split(",");
		double t = Double.parseDouble(tokens[1]);
		if(trueValue==null){ trueValue=t; }
		//else if(trueValue!=t){ throw new RuntimeException("True value unmatching!\nkey ["+key+"] "+trueValue+" vs "+t+"\n"); }

		sum += Double.parseDouble(tokens[0]);
		count++;
	    }
	    //if(count!=this.treesCount){ throw new RuntimeException("Counts wrong!\nkey ["+key+"]"+count+" vs "+this.treesCount+"\n"); }
	    context.write(NullWritable.get(), new Text(key+","+(sum/count)+","+trueValue) );
	}
    }

    private static void calcAvg(String odirPath, int treesCount) throws Exception{
	Configuration conf = new Configuration();
	conf.set("treesCount", Integer.toString(treesCount));
	
	Job job = new Job(conf, "calcAvg");
	job.setJarByClass(WekaForest.class);
	job.setMapperClass(WekaForest.AvgMap.class);
	job.setReducerClass(WekaForest.AvgReduce.class);
	job.setNumReduceTasks(1);
	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(Text.class);
	//job.setInputFormatClass(UnsplittableTextInputFormat.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job, new Path(odirPath+"/step01"));
	FileOutputFormat.setOutputPath(job, new Path(odirPath+"/step02"));
	if(! job.waitForCompletion(true)){ System.exit(1); }
    }


    public static void test(double testProportion) throws Exception{
	File headerFile = new File("data/output/part01-2/header/tt.header");
	File dataFile = new File("data/output/part01-2/data/tt.data.head");

	FileInputStream fis = new FileInputStream(dataFile);
	CharSequence[] css = Tools.splitInputStreamByLine(fis, Tools.asciiCharsetName, 0.2);
	fis.close();

	InputStream trainIS = new SequenceInputStream(new FileInputStream(headerFile), new ByteArrayInputStream(css[1].toString().getBytes(Tools.asciiCharsetName)));
	Instances trainData = DataSource.read(trainIS);
	trainIS.close();
	trainData.setClass(trainData.attribute("Agelaius_phoeniceus"));
	RandomTree tree = new RandomTree();
	tree.setOptions(new String[]{"-K","0","-M","50"});
	tree.buildClassifier(trainData);

	InputStream testIS = new SequenceInputStream(new FileInputStream(headerFile), new ByteArrayInputStream(css[0].toString().getBytes(Tools.asciiCharsetName)));
	Instances testData = DataSource.read(testIS);
	testData.setClass(testData.attribute("Agelaius_phoeniceus"));
	testIS.close();

	double[] predictions = Tools.classify(tree,testData);
	//System.out.println(predictions.length);
	//for(double d: predictions){System.out.println(d); }
	System.out.println(Tools.join("\n",Tools.asList(predictions)));
    }

    public static void test2(int rowCount) throws IOException{
	File headerFile = new File("data/output/part01-2/header/tt.header");
	File dataFile = new File("data/output/part01-2/data/tt.data");

	BufferedReader r = new BufferedReader(new FileReader(headerFile));
	int lineno=0;
	for(; r.readLine()!=null; lineno++);
	r.close();

	r = new BufferedReader(new FileReader(dataFile));
	for(; r.readLine()!=null; lineno++);
	r.close();
	System.out.println(lineno);
    }

    public static void main(String[] args) throws Exception{
	//test(0.2);
	//runWeka(args); // output, M, N, X, treesCount
	calcAvg(args[2],Integer.parseInt(args[6]) );
	//test2(10000);
    }
    
}