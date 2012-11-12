package edu.neu.cs6240.zhoukang;


import weka.core.Instances;
import weka.core.converters.ArffSaver;

public class EnhancedArffSaver extends ArffSaver {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public String getHeader() {
		Instances data = getInstances();
		Instances header = new Instances(data, 0);
		return header.toString();
	}

	public String getData() {
		StringBuilder sb = new StringBuilder();
		Instances data = getInstances();
		for (int i = 0; i < data.numInstances(); i++) {
			sb.append(data.instance(i).toString() + "\n");
		}

		return sb.toString();
	}

	
}
