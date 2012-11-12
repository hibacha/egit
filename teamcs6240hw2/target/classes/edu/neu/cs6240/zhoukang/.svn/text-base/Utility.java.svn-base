package edu.neu.cs6240.zhoukang;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Random;

import org.apache.hadoop.io.Text;

import weka.core.Instances;
import weka.core.Utils;

public class Utility {

	public static String ascii = "us-ascii";

	public static void changeCharsetType(String charset) {
		ascii = charset;
	}

	public static InputStream getByteStreamFromText(Text text) {

		return new ByteArrayInputStream(text.toString().getBytes(
				Charset.forName("us-ascii")));
	}

	public static InputStream getByteStreamFromText(String text) {

		return new ByteArrayInputStream(text.getBytes(Charset
				.forName("us-ascii")));
	}

	public static int generateRandom(int endPoint) {
		Random rdm = new Random();
		return rdm.nextInt(endPoint) + 1;

	}

	public static String assembleHeader(String attr) {

		StringBuffer text = new StringBuffer();

		text.append(Instances.ARFF_RELATION).append(" ")
				.append(Utils.quote("team")).append("\n\n");
		text.append(attr);
		text.append("\n").append(Instances.ARFF_DATA).append("\n");

		return text.toString();
	}
}
