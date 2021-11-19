import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();//加载配置文件,如有改动则覆盖默认配置
        Job job = Job.getInstance(conf);//根据配置信息实例化job对象
        job.setJarByClass(Driver.class); //设置job的主入口程序
        //设置mapper类
        job.setMapperClass(MyMapper.class);
        //设置reducer类
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("/home/hadoop/Documents/data"));
        FileOutputFormat.setOutputPath(job, new Path("/home/hadoop/Documents/dataOutput"));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
