package example.airline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class DepartureDelayCount {
    public static void main(String[] args)  {
        Configuration conf = new Configuration();
        
        if (args.length != 2) {
            System.err.println("Usage : DepartureDelayCount <input> <output>");
            System.exit(2);
        }
        
        //잡이름 설정
        try {
            
            //이미 있다면 삭제처리 해주자. 
            FileSystem hdfs = FileSystem.get(conf);
            if (hdfs.exists(new Path(args[1]))) {
                hdfs.delete(new Path(args[1]), true);
            }                 

            Job job = Job.getInstance(conf, "DepartureDelayCount");            
            
            //입출력 데이터 경로 설정 
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));            
            
            //잡 클래스 설정 
            job.setJarByClass(DepartureDelayCount.class);            
            
            //매퍼 클래스 설정
            job.setMapperClass(DepartureDelayCountMapper.class);
            
            //리듀스 클래스 설정
            job.setReducerClass(DelayCountReducer.class);
            
            //입출력 데이터 포멧설정
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            
            //출력키 설정
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.waitForCompletion(true);
                        
        } catch (Exception e) {            
            e.printStackTrace();
        }
    }
}
