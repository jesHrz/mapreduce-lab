package lab1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapreduceTest {
    private static final String HADOOP_HOME = System.getenv("HADOOP_HOME");
    public static void test1_run() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        config.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
        Job job = new Job(config, "MapreduceTest_1");
        job.setJarByClass(MapreduceTest.class);
        job.setMapperClass(MapreduceTest.Test1_Mapper.class);
        job.setReducerClass(MapreduceTest.Test1_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("test/lab1_test1_input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void test2_run() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        config.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
        Job job = new Job(config, "MapreduceTest_2");
        job.setJarByClass(MapreduceTest.class);
        job.setMapperClass(MapreduceTest.Test2_Mapper.class);
        job.setReducerClass(MapreduceTest.Test2_Reducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path("test/lab1_test2_input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void test3_run() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        config.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
        Job job = new Job(config, "MapreduceTest_3");
        job.setJarByClass(MapreduceTest.class);
        job.setMapperClass(MapreduceTest.Test3_Mapper.class);
        job.setReducerClass(MapreduceTest.Test3_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("test/lab1_test3_input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /*编程实现文件的合并和去重*/
    public static class Test1_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new Text(""));
        }
    }

    public static class Test1_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    /*编程实现对输入文件的排序*/
    public static class Test2_Mapper extends Mapper<Object, Text, LongWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(Long.parseLong(value.toString())), new Text(""));
        }
    }

    public static class Test2_Reducer extends Reducer<LongWritable, Text, IntWritable, LongWritable> {
        private static IntWritable rank = new IntWritable(0);

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text it : values) {
                rank = new IntWritable(rank.get() + 1);
                context.write(rank, key);
            }
        }
    }

    /*对指定的表格进行信息挖掘*/
    public static class Test3_Mapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] name = value.toString().split(" ");
            context.write(new Text(name[0]), new Text("p;" + name[1]));
            context.write(new Text(name[1]), new Text("c;" + name[0]));
        }
    }

    public static class Test3_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> chs = new ArrayList<>();
            List<String> fas = new ArrayList<>();
            for (Text value : values) {
                String[] val = value.toString().split(";");
                if (val[0].equals("p")) {
                    fas.add(val[1]);
                } else if (val[0].equals("c")) {
                    chs.add(val[1]);
                }
            }
            for (String ch : chs) {
                for (String fa : fas) {
                    context.write(new Text(ch), new Text(fa));
                }
            }
        }
    }
}
