package edu.uprm.cse.bigdata.p1exam1;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Created by ubuntu on 2/6/17.
 */
public class KeywordsToTweetsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
       
        
        String ids = "";
        
        for (Text value : values ){
        	ids = ids + ",  " + value;
            
        }
       
        context.write(key, new Text(ids));
    }
}
