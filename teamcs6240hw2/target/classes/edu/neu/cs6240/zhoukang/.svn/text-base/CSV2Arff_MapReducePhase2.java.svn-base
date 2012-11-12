package edu.neu.cs6240.zhoukang;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import weka.core.Attribute;
import weka.core.Instances;
import weka.core.Utils;
import weka.core.converters.ArffLoader;

public class CSV2Arff_MapReducePhase2 {

	// hadoop global counter
	static enum CounterPhase2 {
		LINE, FIEDLD, ALL
	};

	/**
	 * 
	 * @author zhouyf
	 * 
	 */
	public static class CustomizedPartitioner extends
			Partitioner<IntWritable, Text> {
		public int getPartition(IntWritable key, Text value, int numPartitions) {
			// just partition by the first character of each key since
			// that's
			// how we are grouping for the reducer

			int headerOnlyNum = 2;
			if (key.get() >= 0 && key.get() < 1000) {
				return 0;
			} else if (key.get() >= 1000) {
				return 1;
			} else {
				return generateContentReduceNum(headerOnlyNum, numPartitions);
			}
		}

		public int generateContentReduceNum(int headerOnlyNum,
				int totalPartitonsNum) {
			// exclusive numPartition
			int random = new Random()
					.nextInt(totalPartitonsNum - headerOnlyNum);
			return random + headerOnlyNum;

		}

	}

	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		private EnhancedCSVLoader csvLoader = new EnhancedCSVLoader();

		/**
		 * get the attribute type string from phase1
		 */
		private String getAttrTypesFromCache(Configuration conf)
				throws IOException {

			Path[] paths = DistributedCache.getLocalCacheFiles(conf);
			String filePath = "";
			for (Path path : paths) {
				if (path.toString().indexOf(Constant.FILE_ATTRTYPE_EXTENSION) > 0) {
					filePath = path.toString();
					break;
				}
			}
			BufferedReader fis = null;
			StringBuffer sb = new StringBuffer();
			try {
				fis = new BufferedReader(new FileReader(filePath));
				String line = "";
				int count = 1;
				while ((line = fis.readLine()) != null) {

					if (line.startsWith("nominal"))
						sb.append(String.valueOf(count) + ",");
					count++;
				}
				System.out.println(count);
				fis.close();
			} catch (FileNotFoundException io) {

			}
			// only read first line of the CSV header

			return sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";
		}

		/**
		 * helper function to get CSV
		 * 
		 * @throws IOException
		 */
		private String getCSVFileFromCache(Configuration conf)
				throws IOException {

			Path[] paths = DistributedCache.getLocalCacheFiles(conf);
			String filePath = "";
			for (Path path : paths) {
				if (path.toString().indexOf("csv") > 0) {
					filePath = path.toString();
					break;
				}
			}
			BufferedReader fis = new BufferedReader(new FileReader(filePath));
			// only read first line of the CSV header
			String header = fis.readLine();
			fis.close();
			return header;
		}

		/**
		 * read CSV.header from cache add to LINE
		 */
		protected void setup(Context context) {

			try {
				header = getCSVFileFromCache(context.getConfiguration());
				csvLoader.setHeader(header);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				attrsType = getAttrTypesFromCache(context.getConfiguration());
				csvLoader.forceAttributeType(attrsType);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		private String header = "";
		private String attrsType = "";
		private int lineNumCounter = 0;

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (validate(value.toString())) {
				context.getCounter(CounterPhase2.LINE).increment(1);
				// increase number for flushing
				lineNumCounter++;
				csvLoader.readLine(value.toString().trim());
				if (lineNumCounter == 200) {
					flush4Mapout(context);
					try {
						resetAll();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		}

		private void resetAll() throws Exception {

			csvLoader.reset();
			lineNumCounter = 0;
			csvLoader.setHeader(header);
			csvLoader.forceAttributeType(attrsType);
			System.gc();

		}

		private boolean validate(String record) {
			int currentNum = record.split(",").length;
			if (currentNum == csvLoader.getFieldNum()) {
				return true;
			} else {
				return false;
			}
		}

		private void flush4Mapout(Context context) throws IOException,
				InterruptedException {
			Instances data = csvLoader.getDataSetFromLines();
			EnhancedArffSaver saver = new EnhancedArffSaver();
			saver.setInstances(data);

			splitHeader(saver.getHeader(), context);

			// context.write(new IntWritable(0), new Text(saver.getHeader()));
			context.write(new IntWritable(-1 * Utility.generateRandom(1000)),
					new Text(saver.getData()));
		}

		/**
		 * TODO:read attribute type
		 */
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// context.write(new IntWritable(1), new Text(sb.toString()));

			flush4Mapout(context);
			// Instances data = csvLoader.getDataSetFromLines();
			//
			// EnhancedArffSaver saver = new EnhancedArffSaver();
			// saver.setInstances(data);
			//
			// // TODO:random the mapout key for data content
			// splitHeader(saver.getHeader(), context);
			//
			// // context.write(new IntWritable(0), new
			// Text(saver.getHeader()));
			// context.write(new IntWritable(-1 * Utility.generateRandom(1000)),
			// new Text(saver.getData()));
		}

		public void splitHeader(String fullHeader, Context context)
				throws IOException, InterruptedException {
			String[] lines = fullHeader.split("\n");
			int counter = 1;

			// emit relation metadata
			StringBuffer headerRelation = new StringBuffer();
			headerRelation.append(Instances.ARFF_RELATION).append(" ")
					.append(Utils.quote("hw2phase2")).append("\n\n");
			context.write(new IntWritable(0),
					new Text(headerRelation.toString()));
			for (int i = 0; i < lines.length; i++) {
				if (lines[i].indexOf("@attribute") > -1) {

					context.write(new IntWritable(counter++),
							new Text(lines[i]));
					// System.out.println(counter++ +":"+lines[i]);
				}

			}
			context.write(new IntWritable(Integer.MAX_VALUE), new Text("\n"
					+ Instances.ARFF_DATA + "\n\n"));

		}
	}

	// TODO: merge Arff Header
	public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

		private void mergeHash(HashMap<String, String> hash, String str) {
			if (!hash.containsKey(str)) {
				hash.put(str, "");
			}

		}

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int id = key.get();
			Attribute att = null;
			// for header reduce
			if (id == 0 || id == Integer.MAX_VALUE) {
				for (Text attr : values) {
					// TODO
					context.write(attr, new Text(""));
					break;
				}

			} else if (id > 0) {
				ArffLoader arffLoader = new ArffLoader();
				HashMap<String, String> hash = new HashMap<String, String>();
				Boolean isNominal = false;

				for (Text attr : values) {

					// assemble one attribute as string like a real arff header
					String header = Utility.assembleHeader(attr.toString());
					arffLoader.reset();

					// arff loader to load the header
					arffLoader.setSource(Utility.getByteStreamFromText(header));
					Instances instances = arffLoader.getDataSet();

					// only one attribute
					att = instances.attribute(0);
					if (att.isNominal()) {
						isNominal = true;
						Enumeration<String> enumcandidate = att
								.enumerateValues();
						while (enumcandidate.hasMoreElements()) {
							mergeHash(hash, enumcandidate.nextElement());
						}
					}

				}
				if (!isNominal) {
					context.write(new Text(att.toString()), new Text(""));
				} else {
					Set<String> set = hash.keySet();
					Iterator<String> it = set.iterator();
					StringBuffer sb = new StringBuffer();
					while (it.hasNext()) {
						sb.append(Utils.quote(it.next()) + ",");
					}

					String finalValue = sb.subSequence(0, sb.length() - 1)
							.toString();
					String mergedFinalValue = "@attribute " + Utils.quote(att.name()) + " {"
							+ finalValue + "}";
					context.write(new Text(mergedFinalValue), new Text(""));
				}

			} else {

				for (Text txt : values) {
					context.write(new Text(txt), new Text(""));
				}
			}
		}

	}

	/**
	 * takes 2 arguments: - CSV input file - ARFF output file
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "phase2");
		job.setJarByClass(CSV2Arff_MapReducePhase2.class);

		// parse third party library and customized reduce task
		GenericOptionsParser optionParser = new GenericOptionsParser(
				job.getConfiguration(), args);
		String[] otherArgs = optionParser.getRemainingArgs();

		// set mapper, reducer and partitioner
		job.setMapperClass(Map.class);
		job.setPartitionerClass(CustomizedPartitioner.class);
		job.setReducerClass(Reduce.class);

		// specify types of map output key and key
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		//
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// int reducetask = Integer.valueOf(job.getConfiguration().get(
		// "mapred.reduce.tasks"));

		job.setNumReduceTasks(10);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		// set header file and attributes type file to distributed cache
		DistributedCache.addCacheFile(new Path(otherArgs[2]).toUri(),
				job.getConfiguration());
		DistributedCache.addCacheFile(new Path(otherArgs[3]).toUri(),
				job.getConfiguration());
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
