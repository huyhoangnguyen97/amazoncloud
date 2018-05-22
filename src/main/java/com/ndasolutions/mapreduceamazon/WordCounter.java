package com.ndasolutions.mapreduceamazon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;


public class WordCounter {

    /**
     * Một đối tượng thừa kế lớp Mapper thực hiện giai đoạn "map" trong map reduce.
     * hàm map với các tham số đã được quy ước chặt chẽ.
     * Lớp đối tượng TokenizerMapper này sẽ được khai báo để cấu hình cho đối tượng mapperClass của 1 Job
     * thông qua Setter setMapperClass() (sẽ được gọi ở hàm main)
     */
    public static class TokenizeMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            // gán cho từng từ từ dữ liệu đọc được, mỗi từ 1 token để phân biệt
            StringTokenizer st = new StringTokenizer(value.toString());

            // khởi tạo đối tượng tượng trưng cho key
            Text wordOut = new Text();

            // giá trị là 1 khi gán cho mỗi từ
            IntWritable one = new IntWritable(1);

            // lặp và xét từng token kế tiếp nhau
            // tương ứng với mỗi từ khác nhau và gán giá trị là 1 cho mỗi từ
            // đếm các từ có 4 kí tự trở lên
            while (st.hasMoreTokens())
            {
                wordOut.set(st.nextToken());
                if(wordOut.getLength() >=4)
                {
                    // gán cặp key-value
                    context.write(wordOut, one);
                }
            }
        }
    }

    /**
     * Một đối tượng thừa kế lớp Reducer thực hiện giai đoạn "reduce" trong map reduce.
     * hàm reduce với các tham số đã được quy ước chặt chẽ.
     * Lớp đối tượng SumReducer này sẽ được khai báo là 1 trong các tham số để khởi tạo
     * đối tượng SumReducer có nhiệm vụ tính tổng tất cả value của mỗi key, tức là
     * đếm từ đó xuất hiện bao nhiêu lần trong các file text, và sau đó ghi ra file text
     * ở Arguments cuối cùng (chính là thư mục Output)
     */
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text term, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException {
            // tính tổng tất cả các values thông qua biến count
            int count = 0;
            Iterator<IntWritable> iterator = ones.iterator();
            while (iterator.hasNext()){
                count++;
                iterator.next();
            }
            IntWritable output = new IntWritable(count);

            // ghi dữ liệu vào thư mục output
            context.write(term, output);
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // khởi tạo đối tượng cấu hình
        // sẽ tự động lấy cấu hình khi Submit Job
        Configuration conf = new Configuration();

        // khởi tạo job dựa trên các cấu hình
        // tên của job được đặt là "Word Count"
        Job job = Job.getInstance(conf, "Word Count");

        // cấu hình cho 1 job
        // lớp đối tượng đóng gói JAR (chính là lớp đối tượng hiện tại)
        job.setJarByClass(WordCounter.class);

        // cài đặt đối tượng Mapper: TokenizerMapper
        // hàm map
        job.setMapperClass(TokenizeMapper.class);

        // cài đặt đối tượng Reducer: SumReducer
        // hàm reduce
        job.setReducerClass(SumReducer.class);


        job.setNumReduceTasks(10);

        // Đặt kiểu dữ liệu dành cho OutputKey
        job.setOutputKeyClass(Text.class);

        // cài đặt đối tượng để biểu diễn value của key: IntWritable
        job.setOutputValueClass(IntWritable.class);

        // ở đây cấu hình cho đường dẫn đến 2 file text để đọc
        // là tham số đầu tiên và thứ 2
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        // tham số cuối cùng là đường dẫn thư mục out put
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // kiểm tra nếu job hoàn tất thành công thì kết thúc chương trình
        boolean status = job.waitForCompletion(true);
            if(status){
                System.exit(0);
            }
            else{
                System.exit(1);
            }
    }
}
