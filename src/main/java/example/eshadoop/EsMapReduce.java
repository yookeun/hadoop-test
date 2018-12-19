package example.eshadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import example.airline.DepartureDelayCountMapper;

public class EsMapReduce extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        // Start the WordCount MapReduce application
        int res = ToolRunner.run(new Configuration(), new EsMapReduce(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage :  EsMapReduce <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);
        conf.set("es.nodes", "localhost:9200");
        conf.set("es.resource", "eshadoop/_doc");
        conf.set("es.batch.size.entries", "1");

        Job job = Job.getInstance(conf, "EsMapReduce");
        // 입출력 데이터 경로 설정
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // job.setSpeculativeExecution(false);

        // 잡클래스설정
        job.setJarByClass(EsMapReduce.class);

        // Map class
        job.setMapperClass(DepartureDelayCountMapper.class);
        // Reducer class
        // job.setReducerClass(DelayCountReducer.class);
        job.setReducerClass(EsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // output format
        job.setOutputFormatClass(EsOutputFormat.class);

        // output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        return (job.waitForCompletion(true) ? 0 : 1);

    }
}
