# hadoop-map-reduce

# Distributed GREP Using Map-Reduce
We have to find occurences of the word 'door' And output the fileName, line byte offset and the line text.
So, for each record that is send to the mapper (by LineRecordReader) we have ByteOffset of the line as the key and
the line-text as the value.
The filename can be retrieved from the context of the mapper record.


# GREP with ZIP Files Using Map-Reduce
We have to find occurences of the word 'door', but here we have to parse the zip input file.
We create our custom InputFormat which makes us write our own Chunk splitting logic.
Also, we create our custom record reader which further splits the chunk into multiple records to be processed by a mapper.

In the getSplits() of NYUZInputFormat we provide the logic to split the zip file into one split which has all files inside the zip.
In the NYUZRecordReader we provide the logic of splitting one chunk into multiple records. We use the java.utils.zip.ZipInputStream
to decompress each record and convert it into ByteWritable and send <K,V> to the mapper where K = Name of the file, V = Decompressed content of the file in ByteWritable.
In the mapper method, we then convert this ByteWritable to String to further process it. Now, since each record to the mapper is the full txt in the file, we can iterate over the lines
and use a counter to find the line and line-number which has the word 'door'.
The key to the mapper is the name of the file.

# JSON Record with Map-Reduce
We assume the json file has this ideal json multi-line structure:
{...
....
},
{....
    {..
    ...
    }
},
{....
    {..
    ...
    }
}

Inside each JSON file are multiple json records which are multi-line.
We have a Custom input format, which creates splits of each file.
We create a Custom Record Reader to split each json file into a record. To achieve this, we check each line and increase counter on
every opening curly brackets '{' and decrease the counter on every closing brackets '}'. When the counter becomes 0 we know that the
record is complete and push the record into key, value pair.
* We used BufferedReader so as to not read the whole file into memory. BufferedReader helps us achieve this
by reading one line at a time in memory.

Note: I have provided sample json files with this package.
