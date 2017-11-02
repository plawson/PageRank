package ooc.exercice1.pagerank.mapper;

import ooc.exercice1.pagerank.counter.PGCounters;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LineCountMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    /**
     * Mapper to count the number of lines in an input file whatever the input line format.
     * There is no output data.
     * @param key Line number il the file
     * @param value Input line
     * @param context Mapper context object
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Counter counter = context.getCounter(PGCounters.CountersEnum.NUMBER_OF_LINES);
        counter.increment(1);
    }
}
