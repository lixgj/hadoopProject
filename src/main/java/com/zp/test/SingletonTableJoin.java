package com.zp.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SingletonTableJoin {

    public static class STMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String childName = new String();
            String parentName = new String();
            String relationType = new String();
            String line = value.toString();
            String[] values = line.split(" ");

            if(values.length >= 2)
            {
                if(values[0].compareTo("child") != 0){
                    childName = values[0];
                    parentName = values[1];
                    relationType = "1";
                    context.write(new Text(parentName), new Text(relationType + "@" + childName));
                    relationType = "2";
                    context.write(new Text(childName), new Text(relationType + "@" + parentName));
                }
            }
        }
    }

    public static class STReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> grandChild = new ArrayList<String>();
            List<String> grandParent = new ArrayList<String>();
            Iterator<Text> it = values.iterator();
            while(it.hasNext()){
                String s = it.next().toString();
                String[] record = s.split("@");
                if(record.length == 0) continue;
                if(record[0].equals("1")){
                    grandChild.add(record[1]);
                }else if(record[0].equals("2")){
                    grandParent.add(record[1]);
                }
            }

            if(grandChild.size() != 0 && grandParent.size() != 0){
                for(int i=0; i < grandChild.size(); i++){
                    for(int j=0; j < grandParent.size(); j++){
                        context.write(new Text(grandChild.get(i)), new Text(grandParent.get(j)));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.out.println("Usage: SingletonTableJoin <in> <out>");
        }
        Job job = Job.getInstance(conf, "SingletonTableJoin Job");
        job.setJarByClass(SingletonTableJoin.class);
        job.setMapperClass(STMapper.class);
        job.setReducerClass(STReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }
}
