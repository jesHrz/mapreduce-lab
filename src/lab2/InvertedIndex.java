package lab2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class InvertedIndex {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job();
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndex.InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndex.InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndex.InvertedIndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.addCacheFile(new Path("test/stop_words_eng.txt").toUri());
        FileInputFormat.addInputPath(job, new Path("test/lab2_input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        private final Text one = new Text("1");
        private Set<String> stopWords;
        private String fileName;

        @Override
        protected void setup(Context context) throws IOException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String[] _path = split.getPath().toString().split("/");
            fileName = _path[_path.length - 1];

            URI[] files = Job.getInstance(context.getConfiguration()).getCacheFiles();
            String line;
            stopWords = new TreeSet<String>();
            for (URI file : files) {
                Path path = new Path(file.getPath());
                BufferedReader in = new BufferedReader(new FileReader(path.getName()));
                while ((line = in.readLine()) != null) {
                    StringTokenizer it = new StringTokenizer(line);
                    while (it.hasMoreTokens()) {
                        stopWords.add(it.nextToken().trim());
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().trim().split("\\W");
            for (String word : words) {
                word = word.toLowerCase();
                if (!stopWords.contains(word) && !word.equals("")) context.write(new Text(fileName + "#" + word), one);
            }
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }
            String[] splits = key.toString().split("#");
            context.write(new Text(splits[1]), new Text(splits[0] + "#" + Integer.toString(sum)));
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            StringBuilder valueBuilder = new StringBuilder();
            List<String> allFiles = new ArrayList<>();
            for (Text val : values) {
                sum += Integer.parseInt(val.toString().split("#")[1]);
                allFiles.add(val.toString());
            }
            Collections.sort(allFiles);
            for (String file : allFiles) {
                valueBuilder.append("<").append(file.replace('#', ',')).append(">;");
            }
            context.write(key, new Text(valueBuilder.toString() + "<total," + sum + ">."));
        }
    }
}
