package com.example.q3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/*** Custom Hadoop Record Reader : zipped file
 *
 * We want to produce (K,V) pairs where
 *    K = filename inside the zip file
 *    V = bytes corresponding to the file
 *
 * ***/
public class NYUZRecordReader extends RecordReader<Text, BytesWritable> {
    private FSDataInputStream fsDataInputStream;
    private ZipInputStream zip;
    private Text key;
    private BytesWritable value;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        Path path = ((FileSplit) inputSplit).getPath();
        FileSystem fs = path.getFileSystem(conf);
        fsDataInputStream = fs.open(path);
        zip = new ZipInputStream(fsDataInputStream);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        ZipEntry fileEntry = zip.getNextEntry();
        if (fileEntry == null)
            return false;
        key = new Text(fileEntry.getName());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] temp = new byte[8192];
        int length;
        while ((length = zip.read(temp)) >= 0) {
            outputStream.write(temp, 0, length);
        }
        zip.closeEntry();
        value = new BytesWritable(outputStream.toByteArray());
        return true;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 1.0F;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public void close() throws IOException {
    }
}
