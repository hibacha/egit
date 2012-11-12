package edu.neu.cs6240.zhoukang;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class UnsplittableTextInputFormat extends TextInputFormat{

    @Override
    protected boolean isSplitable(JobContext context, Path file) { return false; }
}