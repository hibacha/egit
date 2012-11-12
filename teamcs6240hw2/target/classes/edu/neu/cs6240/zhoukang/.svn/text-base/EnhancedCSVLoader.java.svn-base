package edu.neu.cs6240.zhoukang;

import java.io.IOException;

import weka.core.Instances;
import weka.core.converters.CSVLoader;

public class EnhancedCSVLoader extends CSVLoader {

	@Override
	public void reset() throws IOException {
		// TODO Auto-generated method stub
		super.reset();
		//make a new StringBuilder
		sb=new StringBuilder(2000);
		System.gc();
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -1912559655900011543L;

	private StringBuilder sb = new StringBuilder(2000);
	private int fieldNum=0;
	
	
	public void setHeader(String header){
		this.readLine(header);
		String[] array=header.split(",");
		fieldNum=array.length;
		
	}

	public int getFieldNum(){
		
		return fieldNum;
	}
	public EnhancedCSVLoader() {
		super();
	}

	/* TODO: */
	/**
	 * 
	 * @param expectedTypes
	 * @throws Exception
	 */
	public void forceAttributeType(String expectedTypes) throws Exception {
		String[] options = generateOptions(expectedTypes);
		this.setOptions(options);
	}

	/* TODO:
	 * Valid options are:

	 -N <range>
	  The range of attributes to force type to be NOMINAL.
	  'first' and 'last' are accepted as well.
	  Examples: "first-last", "1,4,5-27,50-last"
	  (default: -none-)
	 -S <range>
	  The range of attribute to force type to be STRING.
	  'first' and 'last' are accepted as well.
	  Examples: "first-last", "1,4,5-27,50-last"
	  (default: -none-)
	 -D <range>
	  The range of attribute to force type to be DATE.
	  'first' and 'last' are accepted as well.
	  Examples: "first-last", "1,4,5-27,50-last"
	  (default: -none-)
	 -format <date format>
	  The date formatting string to use to parse date values.
	  (default: "yyyy-MM-dd'T'HH:mm:ss")
	 -M <str>
	  The string representing a missing value.
	  (default: ?)
	 -E <enclosures>
	  The enclosure character(s) to use for strings.
	  Specify as a comma separated list (e.g. ",' (default: '"')
	 
    */
	 
	private String[] generateOptions(String expectedTypes) {
		return new String[]{"-N",expectedTypes};
	}

	public void readLine(String line) {
		// TODO Auto-generated method stub
		sb.append(line + "\n");

	}

	public Instances getDataSetFromLines() throws IOException {
		// TODO Auto-generated method stub

		setSource(Utility.getByteStreamFromText(sb.toString()));
		return getDataSet();
	}

}
