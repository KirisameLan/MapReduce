import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Text  数据类型：字符串类型 String
 * reduce阶段的输入类型
 * Text reduce阶段的输出数据类型 String类型
 *
 *
 * 数据在Reducer端会先进行一个排序
 * 默认情况下，是按照Key以及其类型进行排序。
 */
public class MyReducer extends Reducer<Text, Text, Text, Text> {
    private ArrayList<String> record = new ArrayList<>();
    private final Text value = new Text();
    private String name;
    @Override
    protected void reduce(Text key,Iterable<Text> values,Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
        record = new ArrayList<>();
        for (Text val : values){
            String[] fields = StringUtils.split(val.toString(),' ');
            String flag = fields[0];
            if (flag.equals("student")){
                name = fields[1];
            }
            else if (flag.equals("student_course")){
                record.add(fields[1]);
            }
        }
        for (String i:record){
            value.set(name+","+i);
            context.write(key,value);
        }
    }
}
