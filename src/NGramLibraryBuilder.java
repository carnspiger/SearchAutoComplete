import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by caitlin.ye on 5/20/17.
 */
public class NGramLibraryBuilder {


    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        int noGram;
        @Override
        public void setup(Context context){
            Configuration configuration = context.getConfiguration();
            noGram = configuration.getInt("noGram", 5);

        }

        //map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

            //read sentence by sentence
            //(n-1) split
            //write to disk(context.write)
            String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]", " ");
            String[] words = line.split("\\s+"); //split at space, \s needed for raw data

            if(words.length<2){   //don't need words less than 2 chars
                return;
            }

            //I love big data
            for(int i = 0; i<words.length; i++){
                StringBuilder sb = new StringBuilder();
                sb.append(words[i]);
                for(int j = 1; i+j < words.length && j <noGram; j++){
                    sb.append(" ");
                    sb.append(words[j]);
                    context.write(new Text(sb.toString()), new IntWritable(1));
                }
            }

        }
    }



    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        //reduce method
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum =0;
            for(IntWritable value: values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));

        }
    }
}
