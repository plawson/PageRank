package ooc.exercice1.pagerank.reducer;

import ooc.exercice1.pagerank.constants.PageRankConstants;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class MakePageRankReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private static final DecimalFormat df = new DecimalFormat(PageRankConstants.DOUBLE_FORMAT);

    /**
     * Generate the PageRank vector.
     * Input format:
     *                  <col> <vector_col_element * P_matrix_normalized_element>
     * Output format:
     *                  <page#> <PageRank>
     * @param key Vector column
     * @param values Vector column elements to sum up.
     * @param context Reducer context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double sum = 0.0;
        for (DoubleWritable value : values) {
            sum += value.get();
        }

        context.write(key, new Text(df.format(sum)));

    }
}
