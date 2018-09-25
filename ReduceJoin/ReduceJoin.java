import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {
    public static class StudentMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

            String record = value.toString();
            String[] parts = record.split(",");

            // 0 --> student ID
            // 1 --> name
            // 2 --> year of birth

            // requirements: The year of birth is greater than (>) 1989
            if (Integer.parseInt(parts[2]) <= 1989) {
                return;
            }

            // produce record with student id as key and stdnt  name    yearOfBirth as value
            String str = String.format("stdnt\t%s\t%s",parts[1], parts[2]);
            context.write(new Text(parts[0]), new Text(str));
        }
    }

    public static class ScoreMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

            String record = value.toString();
            String[] parts = record.split(",");

            // 0 --> student ID
            // 1 --> score for course 1
            // 2 --> score for course 2'
            // 3 --> score for course 3

            // requirements: Each scores of the courses 1,2,3 is greater than (>) 80
            for (int x = 1; x < 4; x++) {
                if (Integer.parseInt(parts[x]) <= 80) {
                    return;
                }
            }

            String str = String.format("score\t%s\t%s\t%s", parts[1], parts[2], parts[3]);
            context.write(new Text(parts[0]), new Text(str));
        }
    }

    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            String notFound = "score not found";
            String unknown = "unknown";
            String name = unknown;
            String yearOfBirth = unknown;
            String score1 = notFound;
            String score2 = notFound;
            String score3 = notFound;

            for (Text t : values) {
                String parts[] = t.toString().split("\t");

                if (parts[0].equals("score")) {
                    score1 = parts[1];
                    score2 = parts[2];
                    score3 = parts[3];
                } else if (parts[0].equals("stdnt") ) {
                    name = parts[1];
                    yearOfBirth = parts[2];
                }
            }

            // filter out incomplete joins
            if (name.equals(unknown) || score1.equals(notFound)) {
                return;
            }

            String str = String.format("%s\t%s\t%s\t%s\t%s", name, yearOfBirth, score1, score2, score3);
            context.write(key, new Text(str));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Reduce-side join");

        job.setJarByClass(ReduceJoin.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StudentMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ScoreMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        //outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
