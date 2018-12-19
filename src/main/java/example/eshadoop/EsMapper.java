package example.eshadoop;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import example.airline.AirlinePerformanceParser;

public class EsMapper extends Mapper<Object, Text, Text, MapWritable> {
    private MapWritable doc = new MapWritable();    
    private Text outputKey = new Text();
    
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        
        AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
        outputKey.set(parser.getYear() + "" + parser.getMonth());
        if (parser.getArriveDelayTime() > 0) {
            context.write(outputKey, doc);
        }
    }


    

}
