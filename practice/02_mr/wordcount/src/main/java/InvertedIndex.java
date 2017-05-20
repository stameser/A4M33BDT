import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class InvertedIndex {
    public static void main(String[] args) throws Exception  {
        JobConf conf = new JobConf(WordCountV2.class);
        conf.setJobName("inverted index");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(InvertedIndex.Map.class);
        conf.setCombinerClass(InvertedIndex.Reduce.class);
        conf.setReducerClass(InvertedIndex.Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private Text docId = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] vals = line.split("\\t");
            StringTokenizer tokenizer = new StringTokenizer(vals[1]);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                docId.set(vals[0]);
                output.collect(word, docId);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            StringBuffer buffer = new StringBuffer();
            while (values.hasNext()) {
                buffer.append(values.next() + " ");
            }
            Text val = new Text();
            val.set(buffer.toString());
            output.collect(key, val);
        }
    }
}
