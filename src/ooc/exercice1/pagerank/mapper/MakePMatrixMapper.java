package ooc.exercice1.pagerank.mapper;

import ooc.exercice1.pagerank.constants.PageRankConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Set;

public class MakePMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final DecimalFormat df = new DecimalFormat(PageRankConstants.DOUBLE_FORMAT);

    private int numberOfPages;
    private  double probability;

    /**
     * Retrieve the number of pages and th teleportation probability
     * @param context Mapper context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        this.numberOfPages = new Long(context.getConfiguration().getLong(PageRankConstants.NUMBER_OF_PAGES, 0)).intValue();
        if (this.numberOfPages == 0) {
            throw new IllegalStateException(this.getClass().getName() + ": Number of Pages is zero !");
        }
        this.probability = context.getConfiguration().getDouble(PageRankConstants.PROBABILITY, 0.15);
    }

    /**
     * Mapper to create the P matrix and normalize it so that the sum of all elements in a P matrix row
     * is equals to 1.
     * Input format :
     *                  <page#>: <outgoing_links_page#> <outgoing_links_page#> <outgoing_links_page#>...
     * Output format:
     *                  <page#> <P_matrix_normalized_element>,<P_matrix_normalized_element>,<P_matrix_normalized_element>....
     * @param key Line number in file
     * @param record Adjacency matrix with outgoing links only for a given page number
     * @param context Mapper context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {

        String[] lineAndLinks = record.toString().split(":");
        String row = lineAndLinks[0].replaceAll(":", "").trim();

        int numberOfOutgoingLinks;
        Set<String> outgoingLinksSet = new HashSet<>();

        // Help the garbage collector by scoping temporary large objects
        {
            String[] outgoingLinks = lineAndLinks[1].trim().split("\\s");
            numberOfOutgoingLinks = outgoingLinks.length - 1;
            for (int i = 0; i < numberOfOutgoingLinks; i++) {
                outgoingLinksSet.add(outgoingLinks[i].trim());
            }
        }

        // Generate the P Matrix and normalize line vector in the matrix
        StringBuilder sb = new StringBuilder();
        // Help the garbage collector by scoping temporary large objects
        {
            double[] pMatrix = new double[this.numberOfPages];
            double normalizationFactor = 0.0;
            for (int i = 0; i < pMatrix.length; i++) {
                if (outgoingLinksSet.contains("" + i)) {
                    pMatrix[i] = ((1 - this.probability) * (1 / numberOfOutgoingLinks)) + (this.probability / this.numberOfPages);
                } else {
                    pMatrix[i] = this.probability / this.numberOfPages;
                }
                normalizationFactor += pMatrix[i];
            }
            normalizationFactor = (double)1 / normalizationFactor;

            for (int i = 0; i < this.numberOfPages; i++) {

                sb .append(df.format(pMatrix[i] * normalizationFactor)).append(",");
            }
        }

        sb.setLength(sb.length() - 1);
        context.write(new Text(row), new Text(sb.toString()));
    }
}
