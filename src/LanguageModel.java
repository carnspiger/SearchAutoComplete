/**
 * Created by caitlin.ye on 5/20/17.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


    public class LanguageModel {
        public static class Map extends Mapper<LongWritable, Text, Text, Text> {

            int threshold;
            // get the threshold parameter from the configuration
            @Override
            // get threshold parameter from the configuration
            public void setup(Context context) {
                Configuration configuration = context.getConfiguration();
                threshold = configuration.getInt("threshold", 10); //sets threshold for line 56
            }


            @Override
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                //this is cool\t20
                //split between n-1 and n
                //key = n-1 words
                //value = nth word + count
                String line = value.toString().trim(); //TODO why trim?
                String[] words_count = line.split("\\t"); // track what gram number
                if(words_count.length <2) return; //omit words with 1 char or less

                //index0 = this is cool
                //index1 = 20
                String[] words = words_count[0].split("\\s+");
                int count = Integer.parseInt(words_count[1]);

                if(count<threshold)return;  //count needs to be above a threshold

                StringBuilder stringBuilder = new StringBuilder();
                for(int i =0; i <words.length -1; i++){
                    stringBuilder.append(words[i].append(" "));
                }
                String outputKey = stringBuilder.toString().trim();
                String outputValue = words[words.length-1] + "=" + count;

                context.write(new Text(outputKey), new Text(outputValue));

            }
        }

        public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

            int n;
            // get the n parameter from the configuration
            int topK;

            @Override
            public void setup(Mapper.Context context) {
                Configuration configuration = context.getConfiguration();
                topK = configuration.getInt("threshold", 10); //sets threshold for line 56
            }

            @Override   //first two can iterate through keys from mapper, this is girl, this is boy, this is shit
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                //topK
                //write to db after merging

                //this is, <girl = 50, boy = 60>    , see comment next to override  ,  Text key  and Iterable<Text> values
                //treeMap<count, list<word>>  -->  <1000, <shit, beautiful...>>  <500, <sun>> <50, <girl, women>...>

                TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
                for (Text value: values){
                    String curValue = value.toString().trim();  //TODO why trim?
                    String word = curValue.split("=")[0];
                    int count = Integer.parseInt(curValue.split("=")[1]);

                    if(tm.containsKey(count)){
                        tm.get(count).add(word);
                    }
                    else{
                        List<String> list = new ArrayList<String>();
                        list.add(word);
                        tm.put(count, list);
                    }
                }

                //create iterate for tm
                Iterator<Integer> iterator = tm.keySet().iterator();
                for(int i = 0; iterator.hasNext() && i<topK;){
                    int count = iterator.next();
                    List<String> words = tm.get(count);  //word list
                    for(String curWord: words){
                        context.write(new DBOutputWritable(key.toString(), curWord, count), NullWritable.get());   //write to file system, so we can send to db, from DBOutputWritable class
                        i++;  //TODO why is i here, has to do with topK and db, limit with db query

                     }
                }

                //heap <- <count, word> && comparator
                //topK
                // maxHeap ? minHeap?   , can use either but time complexity

            }
        }
    }


