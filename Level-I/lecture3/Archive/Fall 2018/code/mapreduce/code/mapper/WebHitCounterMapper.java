package mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebHitCounterMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text day = new Text();
    private final IntWritable one = new IntWritable(1);
    private String date;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        date = value.toString().split("- - \\[")[1].split(" ")[0].split(":")[0];
        day.set(date);
        context.write(day, one);
    }
}