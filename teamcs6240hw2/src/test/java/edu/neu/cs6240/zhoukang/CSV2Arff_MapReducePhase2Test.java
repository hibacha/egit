package edu.neu.cs6240.zhoukang;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.neu.cs6240.zhoukang.CSV2Arff_MapReducePhase2.Map;

import weka.attributeSelection.ASEvaluation;
import weka.core.Instances;

public class CSV2Arff_MapReducePhase2Test {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testPartition() {
		int assumingPartition = 10;
		CSV2Arff_MapReducePhase2.CustomizedPartitioner partition = new CSV2Arff_MapReducePhase2.CustomizedPartitioner();
		for (int i = 0; i < 100; i++) {
			int random = partition.generateContentReduceNum(2, assumingPartition);
			assertTrue(random >= 1 && random <= assumingPartition - 1);
			System.out.println(random);
		}
	}

	private String getHeader4Testing() throws Exception {

		EnhancedCSVLoader ecsvloader = new EnhancedCSVLoader();

		String[] options = new String[] { "-N", "1,2" };
		ecsvloader
				.setSource(new File(
						"/Users/zhouyf/Stack/workspace/cs6240hw2/target/bank-data.csv"));
		ecsvloader.setOptions(options);

		Instances data = ecsvloader.getDataSet();

		EnhancedArffSaver saver = new EnhancedArffSaver();
		saver.setInstances(data);
		saver.setFile(new File(
				"/Users/zhouyf/Stack/workspace/cs6240hw2/target/bank-data.arff"));

		return saver.getHeader();

	}

	@Test
	public void testSplit() throws Exception{
		String testStr=getHeader4Testing();
		CSV2Arff_MapReducePhase2.Map map=new CSV2Arff_MapReducePhase2.Map();
		map.splitHeader(testStr, null);
		
	}
}
