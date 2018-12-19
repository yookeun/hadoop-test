package example.eshadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EsReducer extends Reducer<Text, IntWritable, Text, MapWritable>{
    private MapWritable result = new MapWritable();
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }       
        result.put(new Text("yearMonth"),  new Text(key.toString()));
        result.put(new Text("count"), new IntWritable(sum));
        context.write(key, result);
    }
}
