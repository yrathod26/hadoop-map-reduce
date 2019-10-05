package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.StringUtils;
import org.jboss.netty.util.internal.StringUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class JSONRecordReader extends RecordReader {
    private LongWritable key;
    private Text value;
    private long currPosition = 0;
    private long totalLength = 0;
    private FSDataInputStream fsinstream;
    private FileSystem fs;
    private Configuration conf;
    private BufferedReader br;
    private int numberOfBrackets = 0;
    private boolean isComplete = false;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        currPosition = fileSplit.getStart();
        totalLength = fileSplit.getLength();
        this.conf = taskAttemptContext.getConfiguration();
        fs = fileSplit.getPath().getFileSystem(conf);
        fsinstream = fs.open(fileSplit.getPath());
        br = new BufferedReader(new InputStreamReader(fsinstream));
        int c;
        while((c = br.read()) != -1){
            currPosition++;
            if('[' == (char) c) {
                break;
            }
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(currPosition>=totalLength || isComplete)
            return false;
        long startPosition = currPosition;
        String outputValue = "";
        while(currPosition<=totalLength){
            String line = br.readLine();
            if(numberOfBrackets==0 && line.contains("]")){
                return false;
            }
            if(line.contains("{")){
                numberOfBrackets++;
            }
            else if(line.contains("}")){
                numberOfBrackets--;
                if(numberOfBrackets==0){
                    key = new LongWritable(startPosition);
                    if(line.contains("]")) {
                        line = line.substring(0,line.indexOf(']'));
                        isComplete = true;
                    }
                    outputValue = outputValue.concat(line);
                    value = new Text(outputValue);
                    return true;
                }
            }
            outputValue = outputValue.concat(line);
            currPosition++;
        }
        return true;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 1.0f;
    }

    @Override
    public void close() throws IOException {

    }
}
