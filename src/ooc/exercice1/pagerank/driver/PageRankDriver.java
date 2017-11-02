package ooc.exercice1.pagerank.driver;

import ooc.exercice1.pagerank.constants.PageRankConstants;
import ooc.exercice1.pagerank.counter.PGCounters;
import ooc.exercice1.pagerank.mapper.*;
import ooc.exercice1.pagerank.reducer.MakePMatrixReducer;
import ooc.exercice1.pagerank.reducer.MakePageRankReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class PageRankDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        PageRankDriver driver = new PageRankDriver();
        int res = ToolRunner.run(driver, args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.setDouble(PageRankConstants.PROBABILITY, 0.15);

        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remaininArgs = optionsParser.getRemainingArgs();
        if (remaininArgs.length != 3 && remaininArgs.length != 5 && remaininArgs.length != 7 && remaininArgs.length != 9) {
            // Command line parameters are : input directory, output directory, nodes file directory, number of iterations, starting iteration
            System.err.println("Usage: PageRankDriver <in> <out> [-nodes nodes_directory_file] [-iter #iterations] [-start starting_iteration]");
            System.exit(2);
        }

        Path nodes = null;
        String iter = null;
        String start = "1";

        List<String> otherArgs = new ArrayList<>();
        for (int i=1; i <remaininArgs.length; i++) {
            if ("-nodes".equals(remaininArgs[i])) {
                nodes = new Path(PageRankConstants.DIRECTORY_PREFIX + remaininArgs[++i]);
            } else if ("-iter".equals(remaininArgs[i])) {
                iter = remaininArgs[++i];
            } else if ("-start".equals(remaininArgs[i])) {
                start = remaininArgs[++i];
            } else {
                otherArgs.add(remaininArgs[i]);
            }
        }

        if (Integer.parseInt(start) <1 && Integer.parseInt(start) > 99) {
            System.err.println("-start must be greater than 0 and less than 100!");
            System.exit(2);
        }

        int numberOfIterations;
        if (iter == null ) {
            numberOfIterations = 1;
        } else {
            numberOfIterations = Integer.parseInt(iter);
        }

        if (Integer.parseInt(start) > numberOfIterations) {
            System.err.println("Start can't be greater than the number of iteration!");
            System.exit(2);
        }

        Path input = new Path(PageRankConstants.DIRECTORY_PREFIX + otherArgs.get(0));
        String output = PageRankConstants.DIRECTORY_PREFIX + otherArgs.get(1);
        Path outputPMatrix = new Path(PageRankConstants.DIRECTORY_PREFIX + otherArgs.get(1) + PageRankConstants.PMATRIX_SUFFIX);

        // get the number of lines
        long numberOfPages = getNumberOfPages(conf, input);

        // Record the number of lines into the configuration
        conf.setLong(PageRankConstants.NUMBER_OF_PAGES, numberOfPages);

        boolean succeeded;
        if (Integer.parseInt(start) == 1) {

            succeeded = generatePMatrix(conf, input, outputPMatrix);
            if (!succeeded) {
                throw new IllegalStateException("P matrix creation failed!");
            }
        }

        succeeded = computePageRank(conf, outputPMatrix, output, numberOfIterations, Integer.parseInt(start));
        if (!succeeded) {
            throw new IllegalStateException("Vector times P matrix failed!");
        }

        return 0;
    }

    /**
     * Return the number of lines in the adjacency matrix file.
     * @param conf Hadoop Configuretion.
     * @param input Input directory containing the adjacency matrix file.
     * @return the number of lines as a long integer.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    private long getNumberOfPages(Configuration conf, Path input) throws IOException, ClassNotFoundException, InterruptedException {

        // Job creation
        Job job = Job.getInstance(conf);
        job.setJobName("Number Of Lines");
        // Driver, mapper, reducer
        job.setJarByClass(PageRankDriver.class);
        job.setMapperClass(LineCountMapper.class);
        job.setNumReduceTasks(0);
        // Keys, values
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        // Input
        FileInputFormat.addInputPath(job, input);

        if (!job.waitForCompletion(true)) {
            throw new IllegalStateException("Job: Number Of Lines failed!");
        }

        // Get the number of lines counter
        Counters counters = job.getCounters();
        Counter numLines = counters.findCounter(PGCounters.CountersEnum.NUMBER_OF_LINES);


        return numLines.getValue();
    }

    /**
     * Creates the P matrix
     * @param conf conf Hadoop Configuretion.
     * @param input Input directory containing the adjacency matrix file.
     * @param outputPMatrix Output directory where the P matrix will be created
     * @return Return a boolean, true if the Job succeds, false otherwise
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    private boolean generatePMatrix(Configuration conf, Path input, Path outputPMatrix) throws IOException, ClassNotFoundException, InterruptedException {

        // Job creation
        Job job = Job.getInstance(conf);
        job.setJobName("P matrix creation");
        // Driver, mapper, reducer
        job.setJarByClass(PageRankDriver.class);
        job.setMapperClass(MakePMatrixMapper.class);
        job.setReducerClass(MakePMatrixReducer.class);
        // Keys, values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // Input, output
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputPMatrix);
        FileSystem fileSystem = FileSystem.newInstance(conf);
        if (fileSystem.exists(outputPMatrix)) {
            fileSystem.delete(outputPMatrix, true);
        }

        return job.waitForCompletion(true);
    }

    /**
     * Compute the PageRank
     * @param conf Hadoop configuration object
     * @param inputPMatrix P matrix
     * @param output Output directory prfix
     * @param numberOfIterations Number of iterations to execute
     * @param start Starting iteration
     * @return A boolean, true in case of success, false otherwise
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private boolean computePageRank(Configuration conf, Path inputPMatrix, String output, int numberOfIterations, int start)
            throws IOException, InterruptedException, ClassNotFoundException {

        String outputPath = null;
        for (int i = start; i < numberOfIterations + 1; i++) {

            conf.setInt(PageRankConstants.CURRENT_ITERATION, i);

            String iterStr = Integer.toString(i);
            iterStr = iterStr.length() == 1 ? "0" + iterStr : iterStr;

            // Job creation
            Job job = Job.getInstance(conf);
            job.setJobName("Vector times P matrix iteration: " + i);

            if (i == start && start > 1) {
                outputPath = output + PageRankConstants.ITER_SUFFIX + (Integer.toString(start -1).length() == 1 ? "0" + (start - 1) : "" + (start - 1));
            }

            FileSystem fileSystem = FileSystem.newInstance(conf);
            if (i != 1) {
                FileStatus[] fileStatus = fileSystem.listStatus(new Path(outputPath));
                for (FileStatus status : fileStatus) {
                    if (!status.getPath().toString().contains(PageRankConstants.SUCCESS_FILE_NAME))
                        job.addCacheFile(status.getPath().toUri());
                }
            }

            outputPath = output + PageRankConstants.ITER_SUFFIX + iterStr;

            // Driver, mapper, reducer
            job.setJarByClass(PageRankDriver.class);
            job.setMapperClass(MakePageRankMapper.class);
            job.setReducerClass(MakePageRankReducer.class);
            // Key, value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            // Input, output
            FileInputFormat.addInputPath(job, inputPMatrix);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            if (fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            if (!job.waitForCompletion(true)) {
                throw new IllegalStateException("Matrix times P matrix iteration " + i + " failed!");
            }

        }

        return true;
    }
}
