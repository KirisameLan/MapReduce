import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN,默认是一行起始偏移量
 * VALUEIN,默认是下一行的文本内容
 * KEYOUT
 * VALUEOUT
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
//这里定义了一个mapper类，其中有一个map方法。MapReduce框架每读到一行数据，就会调用一次这个map方法。
    @Override
    protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException,InterruptedException{
        /*
        其中key是传入map的键值，value是对应键值的value值，context是环境对象变量，供程序访问Hadoop的环境参数
        map方法对输入的键值对进行处理，产生一系列的中间键值对，转换后的中间键值对可以有新的键值类型。
        输入的键值对可根据实际应用设定，例如文档数据记录可将文本文件中的行或数据表格中的行设为key，对应行的类容为value
        */
        Text outKey = new Text();
        Text outValue = new Text();
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String filePath = fileSplit.getPath().toString();
        String line = value.toString();



        if (filePath.contains("student.csv")){
            String[] allS = line.split(",",3);
            outKey.set(allS[0]);
            outValue.set("student"+" "+allS[1]);
        }
        else if (filePath.contains("student_course.csv")){
            String[] allC = line.split(",",4);
            outKey.set(allC[0]);
            outValue.set("student_course" + " " + allC[1]+","+allC[2]);
        }
        context.write(outKey,outValue);
    }
}
