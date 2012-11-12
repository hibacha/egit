package edu.neu.cs6240.zhoukang;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RelativeCountsMapReduce {

	/**
	 * @param args
	 */

	public static class Map extends
			Mapper<LongWritable, Text, TextPair, LongWritable> {

		private static final String STATE = "STATE_PROVINCE";
		// see comparator to make sure this DUMMY sort first within each species
		private static final String DUMMY = "0000-----";
		// hashtable for storing the <attributeName,index>
		private Hashtable<String, Integer> headerHash = new Hashtable<String, Integer>(
				2002);

		private String[] speciesList = null;

		// initialize the header hash table
		private void initHeader(String header) {
			String[] attribute = header.split(",");
			for (int i = 0; i < attribute.length; i++) {
				headerHash.put(attribute[i].trim(), i);
			}

		}

		@Override
		/**
		 * read from cache to get header
		 * read from cache to get species names
		 */
		protected void setup(Context context) throws IOException,
				InterruptedException {

			Path[] paths = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (Path p : paths) {
				if (p.toString().indexOf("header") >= 0) {
					String header = getCacheFileContent(p.toString()).get(0);
					initHeader(header);
				} else if (p.toString().indexOf("species") >= 0) {
					speciesList = getCacheFileContent(p.toString()).get(0)
							.split(",");
				}
			}
		}

		private int occurTimes(String mark) {
			if (mark != null && mark.trim().length() > 0) {
				try {
					int times = Integer.parseInt(mark);
					return times;
				} catch (NumberFormatException e) {
					return 0;
				}
			}
			return 0;
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// split
			String[] values = value.toString().split(",");

			for (String onespec : speciesList) {
				// get the index of current species in the header
				int pos = headerHash.get(onespec.trim());
				// check if the species occur
				int occurtimes = occurTimes(values[pos]);
				// get the observation location
				String state = values[headerHash.get(STATE)];
				//
				if (occurtimes >= 0) {

					context.write(new TextPair(onespec.trim(), state.trim()),
							new LongWritable(occurtimes));
					context.write(new TextPair(onespec.trim(), DUMMY),
							new LongWritable(occurtimes));
				}

			}

		}

		private ArrayList<String> getCacheFileContent(String localPath)
				throws IOException {

			ArrayList<String> rst = new ArrayList<String>();
			BufferedReader fis = null;
			try {
				fis = new BufferedReader(new FileReader(localPath));
				String line = "";
				while ((line = fis.readLine()) != null) {
					rst.add(line);
				}
				fis.close();
			} catch (FileNotFoundException io) {

			}
			return rst;
		}

	}

	public static class Reduce extends
			Reducer<TextPair, LongWritable, Text, Text> {

		private Hashtable<String, Long> speciesTotalHash = new Hashtable<String, Long>();

		//sum the value of 
		private long getSum(Iterable<LongWritable> list) {
			long sum = 0;
			for (LongWritable l : list) {
				sum += l.get();
			}
			return sum;
		}

		@Override
		protected void reduce(TextPair species_state,
				Iterable<LongWritable> list, Context context)
				throws IOException, InterruptedException {
			// Calculate the total occur times before real state
			if (species_state.getState().toString()
					.equals(RelativeCountsMapReduce.Map.DUMMY)) {
				long sum = getSum(list);
				speciesTotalHash
						.put(species_state.getSpecies().toString(), sum);
			} else {
				long sum = getSum(list);

				float fs = (float) speciesTotalHash.get(species_state
						.getSpecies().toString());
				float fst = (float) sum;

				// check if the total occur time is zero
				if (fs == 0) {
					context.write(new Text(species_state.getSpecies() + ","
							+ species_state.getState() + ",0%"), new Text(""));
				} else {
					float computation = fst / fs;
					context.write(
							new Text(species_state.getSpecies() + ","
									+ species_state.getState() + ","
									+ String.valueOf(computation * 100) + "%"),
							new Text(""));
				}

			}
		}


	}

	public static class Comparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public Comparator() {
			super(TextPair.class);
		}

		/**
		 * this customized comparator to guarantee to dummy sort to be first in
		 * one species
		 */
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2,
						firstL2);
				if (cmp != 0) {
					return cmp;
				}
				return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
						b2, s2 + firstL2, l2 - firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		static {
			WritableComparator.define(TextPair.class, new Comparator());
		}
	}

	public static class Combiner extends
			Reducer<TextPair, LongWritable, TextPair, LongWritable> {

		@Override
		protected void reduce(TextPair key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));

		}

	}

	public static class CustomizedPartitioner extends
			Partitioner<TextPair, LongWritable> {
		public int getPartition(TextPair key, LongWritable value,
				int numPartitions) {
			// just partition by the first character of each key since

			int code = key.getSpecies().hashCode();

			return (code >= 0 ? code : -1 * code) % numPartitions;
			// return 100;
		}

	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "job2");
		job.setJarByClass(RelativeCountsMapReduce.class);
        //parse generic argument like third party library
		GenericOptionsParser optionParser = new GenericOptionsParser(
				job.getConfiguration(), args);
		//user-specific argument
		String[] otherArgs = optionParser.getRemainingArgs();

		job.setMapperClass(Map.class);
		job.setPartitionerClass(CustomizedPartitioner.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combiner.class);

		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(LongWritable.class);
		job.setSortComparatorClass(Comparator.class);

		
		DistributedCache.addCacheFile(new Path(otherArgs[2]).toUri(),
				job.getConfiguration());
		DistributedCache.addCacheFile(new Path(otherArgs[3]).toUri(),
				job.getConfiguration());

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
