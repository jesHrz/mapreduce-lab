package lab3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class PageRank_MapReduce {
    private static final String HADOOP_HOME = System.getenv("HADOOP_HOME");
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final double PR_init = 1.0;
        final double d = 0.85;
        final int max_iteration = 10;

        String output = "output_lab3/";
        GraphBuilder.run("test/lab3_input", output + 0, PR_init);
        for (int i = 0; i < max_iteration; ++i) {
            PageRankIterator.run(output + "output" + i, output + "output" + (i + 1), GraphBuilder.N, d);
        }
        RankViewer.run(output + max_iteration, output);
    }

    private static class GraphBuilder {
        private static final Set<String> st = new TreeSet<>();
        public static int N = 0;

        public static void run(String input, String output, double PR_init) throws InterruptedException, IOException, ClassNotFoundException {
            Configuration config = new Configuration();
            config.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
            config.setDouble("PR_init", PR_init);
            Job job = new Job(config, "GraphBuilder");
            job.setJarByClass(GraphBuilder.class);
            job.setMapperClass(GraphBuilderMapper.class);
            job.setReducerClass(GraphBuilderReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
        }

        private static class GraphBuilderMapper extends Mapper<Object, Text, Text, Text> {
            private double PR_init;

            @Override
            protected void setup(Context context) {
                PR_init = context.getConfiguration().getDouble("PR_init", 1.0);
            }

            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String[] split = value.toString().split("\t");
                String u = split[0];
                st.add(u);
                List<String> link = new ArrayList<>();
                for (String v : split[1].split(",")) {
                    link.add(v);
                    st.add(v);
                }
                context.write(new Text(u), new Text(PR_init + "\t" + StringUtils.join(",", link)));
            }
        }

        private static class GraphBuilderReducer extends Reducer<Text, Text, Text, Text> {
            @Override
            protected void setup(Context context) {
                N = st.size();
            }

            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                for (Text value : values) {
                    context.write(key, value);
                }
            }
        }
    }

    private static class PageRankIterator {
        public static void run(String input, String output, int N, double d) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration config = new Configuration();
            config.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
            config.setInt("N", N);
            config.setDouble("d", d);
            Job job = new Job(config, "PageRankIterator");
            job.setJarByClass(PageRankIterator.class);
            job.setMapperClass(PageRankIteratorMapper.class);
            job.setReducerClass(PageRankIteratorReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
        }

        private static class PageRankIteratorMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String[] split = value.toString().split("\t");
                String u = split[0];
                String[] link = split[2].split(",");
                double cur_rank = Double.parseDouble(split[1]);
                for (String v : link) {
                    context.write(new Text(v), new Text(Double.toString(cur_rank / link.length)));
                    context.write(new Text(u), new Text("," + v));
                }
            }
        }

        private static class PageRankIteratorReducer extends Reducer<Text, Text, Text, Text> {
            private int N;
            private double d;

            @Override
            protected void setup(Context context) {
                N = context.getConfiguration().getInt("N", 0);
                d = context.getConfiguration().getDouble("d", 0.85);
            }

            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                double new_rank = 0;
                List<String> link = new ArrayList<>();
                for (Text value : values) {
                    String val = value.toString();
                    if (val.startsWith(",")) {
                        link.add(val.substring(1));
                    } else {
                        new_rank += Double.parseDouble(val);
                    }
                }
                new_rank = new_rank * d + (1.0 - d);
                context.write(key, new Text(new_rank + "\t" + StringUtils.join(",", link)));
            }
        }
    }

    private static class RankViewer {
        public static void run(String input, String output) throws InterruptedException, IOException, ClassNotFoundException {
            Configuration config = new Configuration();
            config.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
            Job job = new Job(config, "RankViewer");
            job.setJarByClass(RankViewer.class);
            job.setMapperClass(RankViewerMapper.class);
            job.setReducerClass(RankViewerReducer.class);
            job.setMapOutputKeyClass(RankViewerDoubleWritable.class);
            job.setMapOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
        }

        private static class RankViewerDoubleWritable extends DoubleWritable {
            @Override
            public int compareTo(DoubleWritable o) {
                return -super.compareTo(o);
            }
        }

        private static class RankViewerMapper extends Mapper<Object, Text, RankViewerDoubleWritable, Text> {
            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String[] split = value.toString().split("\t");
                String u = split[0];
                RankViewerDoubleWritable pageRank = new RankViewerDoubleWritable();
                pageRank.set(Double.parseDouble(split[1]));
                context.write(pageRank, new Text(u));
            }
        }

        private static class RankViewerReducer extends Reducer<RankViewerDoubleWritable, Text, Text, NullWritable> {
            @Override
            protected void reduce(RankViewerDoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                for (Text value : values) {
                    context.write(new Text(String.format("(%s,%.10f)", value.toString(), key.get())), NullWritable.get());
                }
            }
        }
    }
}
