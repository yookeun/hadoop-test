package example.airline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.hadoop.mapreduce.Reducer;

public class DelayCountMultiOutputReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private MultipleOutputs<Text, IntWritable> mos;
    
    //reduce 출력키
    private Text outputKey = new Text();
    
    //redule 출력값
    private IntWritable result = new IntWritable();
    

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        
        //콤머 구분자 분리 
        String[] columns = key.toString().split(",");
        
        //출력키 설정
        //outputKey.set(columns[1] + "," + columns[2]);
        outputKey.set(columns[1]);
        //출발 지연
        if (columns[0].equals("D")) {
            //지연 횟수 합산
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            //출력값 설정
            result.set(sum);
            //출력 데이터 설정
            mos.write("departure", outputKey, result);
            
            
        //도착지연    
        } else {
            //지연 횟수 합산 
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            //출력값 설정
            result.set(sum);
            //출력 데이터 설정
            mos.write("arrival", outputKey, result);
        }        
     
    }


    @Override
    protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {       
        mos = new MultipleOutputs<>(context);
    }

}
