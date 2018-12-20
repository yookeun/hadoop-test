package example.airline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    //작업구분
    private String workType;
    
    //맵 출력값
    private final static IntWritable outputValue = new IntWritable(1);
    
    //맵 출력키
    private Text outputKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        
                
        AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
        
        //출발지연 데이터 출력
        if (workType.equals("departure")) {
            if (parser.getDepartureDelayTime() > 0) {
                outputKey.set(parser.getYearMonth());                
                context.write(outputKey, outputValue);
            }
        } else if (workType.equals("arrival")){
            if (parser.getArriveDelayTime() > 0) {
                outputKey.set(parser.getYearMonth());
                context.write(outputKey, outputValue);
            }
        }
    }

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        workType = context.getConfiguration().get("workType");
    }
    
    
    
    
}
