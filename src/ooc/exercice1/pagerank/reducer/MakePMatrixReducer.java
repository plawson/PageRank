package ooc.exercice1.pagerank.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MakePMatrixReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * Reducer to create the P matrix used in PageRank computation.
     * Input format :
     *                  <page#> <P_matrix_normalized_element>,<P_matrix_normalized_element>,<P_matrix_normalized_element>....
     * Output format :
     *                  <page#> <P_matrix_normalized_element>,<P_matrix_normalized_element>,<P_matrix_normalized_element>....
     * @param key line#,column#
     * @param value Iterator containing one value : the P matrix element
     * @param context Reducer context object
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

        context.write(key, value.iterator().next());
    }
}
