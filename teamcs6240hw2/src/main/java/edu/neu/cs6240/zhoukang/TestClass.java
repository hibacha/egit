package edu.neu.cs6240.zhoukang;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;

import weka.core.Attribute;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
//import org.apache.commons.lang.enums.Enum;
//import com.sun.tools.javac.comp.AttrContextEnv;

public class TestClass {

	/**
	 * @param args
	 */

	public static final String header = "/Users/zhouyf/Stack/cs6240/teamwork/hw02/program/data/input/sample10K/header.csv";
	public static final String content = "/Users/zhouyf/Stack/cs6240/teamwork/hw02/program/data/input/sample10K/10Ksample.csv";

	public static void main(String[] args) throws Exception {

		EnhancedCSVLoader ecsvloader = new EnhancedCSVLoader();
		String line;
		String[] options = new String[] { "-N", "first-last" };
		ecsvloader.setOptions(options);

		BufferedReader br = new BufferedReader(
				new FileReader(
						"/Users/zhouyf/Stack/cs6240/teamwork/hw02/program/data/input/merged.head.csv"));

		while ((line = br.readLine()) != null) {
			ecsvloader.readLine(line);
		}
		br.close();

		// ecsvloader
		// .setSource(new File(
		// "/Users/zhouyf/Stack/hadoop-1.0.3/hw2input/merged.headori.csv"));
		// ecsvloader.setOptions(options);

		Instances data = ecsvloader.getDataSetFromLines();

		EnhancedArffSaver saver = new EnhancedArffSaver();
		System.out.println(data.attribute(0));
		saver.setInstances(data);
		saver.setFile(new File(
				"/Users/zhouyf/Stack/cs6240/teamwork/hw02/program/data/input/merged.head.arff"));

		saver.writeBatch();
		

		System.out.println(saver.getHeader());
		System.out.println("-----------------");

		ArffLoader arffloader = new ArffLoader();
		arffloader
				.setSource(new File(
						"/Users/zhouyf/Stack/cs6240/teamwork/hw02/program/data/input/merged.head.arff"));
		Instances instances = arffloader.getDataSet();
		Attribute att=instances.attribute(1036);
		//Attribute att2=instances.classAttribute();
		// System.out.println(saver.getData());

		// ArffLoader arfloader = new ArffLoader();
		// arfloader
		// .setSource(Utility
		// .getByteStreamFromText("@relation bank-data \n@attribute sex {FEMALE,MALE}\n@attribute city {INNER_CITY,TOWN,RURAL,SUBURBAN}\n@data"));
		// Instances instances = arfloader.getDataSet();
		// Attribute att = instances.attribute(0);
		// System.out.println(att.toString() + att.isNominal());
		//
		// arfloader.reset();
		// arfloader
		// .setSource(Utility
		// .getByteStreamFromText("@relation bank-data \n@attribute city {901,801,855,832}\n@data"));
		// instances = arfloader.getDataSet();
		// att = instances.attribute(0);
		// System.out.println(att.toString() + att.isNominal());
		//
		// Enumeration<String> list = att.enumerateValues();
		// while (list.hasMoreElements()) {
		// System.out.println("value:" + list.nextElement());
		// }
		// att.ordering();
		// System.out.println(att.toString());

		System.out.print(test());

	}

	public static String test() throws IOException {

		BufferedReader fis = null;
		StringBuffer sb = new StringBuffer();
		try {
			fis = new BufferedReader(new FileReader(
					"/Users/zhouyf/Stack/hadoop-1.0.3/part-r-00000"));
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

}
