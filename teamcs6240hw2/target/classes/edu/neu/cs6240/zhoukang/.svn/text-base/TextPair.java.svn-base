package edu.neu.cs6240.zhoukang;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {

	private Text species;
	private Text state;

	public TextPair() {
		set(new Text(), new Text());
	}

	public TextPair(String first, String second) {
		set(new Text(first), new Text(second));
	}

	public TextPair(Text first, Text second) {
		set(first, second);
	}

	public void set(Text first, Text second) {
		this.species = first;
		this.state = second;
	}

	public Text getSpecies() {
		return species;
	}

	public Text getState() {
		return state;
	}

	@Override
	public int hashCode() {
		return species.hashCode() * 163 + state.hashCode();

	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return species.equals(tp.species) && state.equals(tp.state);
		}
		return false;
	}

	@Override
	public String toString() {
		return species + "\t" + state;
	}

	@Override
	public int compareTo(TextPair tp) {
		int cmp = species.compareTo(tp.species);
		if (cmp != 0) {
			return cmp;
		}
		return state.compareTo(tp.state);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		species.write(out);
		state.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		species.readFields(in);
		state.readFields(in);
	}

}