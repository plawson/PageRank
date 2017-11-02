package ooc.exercice1.pagerank.mapper;

import ooc.exercice1.pagerank.constants.PageRankConstants;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class MakePageRankMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private int numberOfPages = 0;
    private double[] vector;

    /**
     * Retrieve the number of pages et the current iteration.
     * For the first iteration, build the vector
     * Otherwise get the vector data generated in the previous iteration from de distributed cache
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        int currentIteration = context.getConfiguration().getInt(PageRankConstants.CURRENT_ITERATION, 1);
        this.numberOfPages = new Long(context.getConfiguration().getLong(PageRankConstants.NUMBER_OF_PAGES, 0)).intValue();
        //System.out.println(this.getClass().getName() + ".setup: Iteration " + this.currentIteration + ", number of pages " + this.numberOfPages);
        if (this.numberOfPages == 0) {
            throw new IllegalStateException(this.getClass().getName() + ": Number of pages is zero!");
        }

        // Create an array to hold the vector
        this.vector = new double[this.numberOfPages];
        // On the first iteration, generate the vector
        if (currentIteration == 1) {
            for (int i = 0; i < this.numberOfPages; i++) {
                this.vector[i] = (double)1 / this.numberOfPages;
            }
        // On subsequent iterations, load the vector form the previously generated vector file
        } else {

            URI[] vectorFiles = context.getCacheFiles();
            if (vectorFiles != null && vectorFiles.length > 0) {
                for (URI vectorFile : vectorFiles) {
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(new Path(vectorFile.getRawPath()))));
                    String vectorElement;
                    while ((vectorElement = bufferedReader.readLine()) != null) {
                        String[] rowAndElement = vectorElement.trim().split("\\s");
                        this.vector[Integer.parseInt(rowAndElement[0].trim())] = Double.parseDouble(rowAndElement[1].trim());
                    }
                }
            } else {
                throw new IllegalStateException(this.getClass().getName() + "No vector file !");
            }
        }

        //System.out.println(this.getClass().getName() + ".setup: Vector size " + this.vector.length + ", vector[1] " + this.vector[1] + ", vector[100] " + this.vector[100]);
    }

    /**
     * Read each line of the normalized P matrix, multiply each P matrix element in a row with the
     * related vector column, then output the each element using the vector column number as key so that the shuffle/sort
     * group them by column for the reducer to sum each column.
     * Input format:
     *                  <row#> <P_matrix_normalized_element>,<P_matrix_normalized_element>,<P_matrix_normalized_element>....
     * Output format:
     *                  <col> <vector_col_element * P_matrix_normalized_element>
     * @param fakeKey
     * @param record
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable fakeKey, Text record, Context context) throws IOException, InterruptedException {

        String matrixRow;
        String[] matrixElementsStr;
        {
            String[] line = record.toString().split("\\s");
            matrixRow = line[0].trim();
            matrixElementsStr = line[1].split(",");
        }

        double newElement;
        for (int i = 0; i < numberOfPages; i++) {
            // Multiply all matrix elements in the matrix row by the vector element whose column number equals
            // the matrix row number
            newElement = this.vector[Integer.parseInt(matrixRow)] * Double.parseDouble(matrixElementsStr[i].trim());
            // Write the computed element for the matrix column so that the reducer can sum up resulting matrix columns
            context.write(new Text(Integer.toString(i)), new DoubleWritable(newElement));
        }
    }


}
