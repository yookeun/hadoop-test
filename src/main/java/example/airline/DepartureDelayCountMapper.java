package example.airline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    //맵출력값
    private final static IntWritable outputValue = new IntWritable(1);
    //맵 출력키 
    private Text outputKey = new Text();
    
    public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
        AirlinePerformanceParser parser = new AirlinePerformanceParser(text);
        
        //출력키 설정
        outputKey.set(parser.getYear() + "" + parser.getMonth());
        
        if (parser.getDepartureDelayTime() >  0) {
            context.write(outputKey, outputValue);            
        }
    }
    
}
