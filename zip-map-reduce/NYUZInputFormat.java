package com.example.q3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Extends the basic FileInputFormat to accept ZIP files.
 * ZIP files are not 'splittable', so we need to process/decompress in place:
 * each ZIP file will be processed by a single Mapper; we are parallelizing files, not lines...
 */
public class NYUZInputFormat extends InputFormat<Text, BytesWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<>();
        Path[] paths = FileInputFormat.getInputPaths(jobContext);
        File file = new File(paths[0].toString().replace("file:",""));
        File[] files = file.listFiles();
        long length = files[0].length();
        splits.add(new FileSplit(new Path(files[0].getPath()),0,length,new String[0]));
        return splits;
    }

    @Override
    public RecordReader<Text,BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException{
        return new NYUZRecordReader();
    }
}
