import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.net.URI;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class BloomFilterJoin extends Configured implements Tool {

    public static class StudentJoinMapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input string into array
            String record = value.toString();
            String[] parts = record.split(",");
            String studentId = parts[0];

            if (studentId == null) {
                return;
            }

            String yearOfBirth = parts[2];

            if (yearOfBirth == null) {
                return;
            }

            // If the year of birth is greater than 1989
            // output the student ID with the value
            if (Integer.parseInt(yearOfBirth) > 1989) {
                outkey.set(studentId);
                outvalue.set("A" + value.toString());
                context.write(outkey, outvalue);
            }
        }
    }

    public static class ScoreJoinMapperWithBloom extends Mapper<Object, Text, Text, Text> {

        private BloomFilter bfilter = new BloomFilter(2_000_000, 7, Hash.MURMUR_HASH);
        private Text outkey = new Text();
        private Text outvalue = new Text();
        private BufferedReader brReader;

        enum MYCOUNTER {
            RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
        }

        @Override
        public void setup(Context context) throws IOException {
            URI[] files = context.getCacheFiles();

            // DataInputStream strm = new DataInputStream(new FileInputStream(files[0].toString()));
            // bfilter.readFields(strm);

            for (URI eachPath : files) {
                context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
                loadScoresBloom(eachPath, context);
            }
        }

        private void loadScoresBloom(URI filePath, Context context) throws IOException {

            String strLineRead = "";

            try {
                brReader = new BufferedReader(new FileReader(filePath.toString()));

                // Read each line, split and load to HashMap
                while ((strLineRead = brReader.readLine()) != null) {

                    String deptFieldArray[] = strLineRead.split(",");

                    // filter out if all values are not above 80
                    if (Integer.parseInt(deptFieldArray[1]) <= 80 || Integer.parseInt(deptFieldArray[2]) <= 80 || Integer.parseInt(deptFieldArray[3]) <= 80) {
                        continue;
                    }

                    if (deptFieldArray[0].equals(null)) {
                        continue;
                    }

                    Key filterKey = new Key(deptFieldArray[0].trim().getBytes());
                    bfilter.add(filterKey);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
            } catch (IOException e) {
                context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
                e.printStackTrace();
            }finally {
                if (brReader != null) {
                    brReader.close();
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

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

            String studentId = parts[0];

            if (studentId == null) {
                return;
            }

            if (bfilter.membershipTest(new Key(studentId.getBytes()))) {
                outkey.set(studentId);
                outvalue.set("B" + value.toString());
                context.write(outkey, outvalue);
            }
        }
    }

    public static class StudentJoinReducer extends Reducer<Text, Text, Text, Text> {
        private static final Text EMPTY_TEXT = new Text("");
        private Text tmp = new Text();
        private ArrayList<Text> listA = new ArrayList<Text>();
        private ArrayList<Text> listB = new ArrayList<Text>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Clear our lists
            listA.clear();
            listB.clear();

            // iterate through all our values, binning each record based on what
            // it was tagged with. Make sure to remove the tag!
            while (values.iterator().hasNext()) {
                tmp = values.iterator().next();

                if (tmp.charAt(0) == 'A') {
                    listA.add(new Text(tmp.toString().substring(1)));
                } else if (tmp.charAt(0) == 'B') {
                    listB.add(new Text(tmp.toString().substring(1)));
                }
            }
            // Execute our join logic now that the lists are filled
            executeJoinLogic(context);
        }

        private void executeJoinLogic(Context context) throws IOException,InterruptedException {
            // If both lists are not empty, join A with B
            if (!listA.isEmpty() && !listB.isEmpty()) {
                for (Text A : listA) {
                    for (Text B : listB) {

                        String[] partsA = A.toString().split(",");
                        String[] partsB = B.toString().split(",");

                        String str = String.format("%s\t%s\t%s\t%s\t%s", partsA[1], partsA[2], partsB[1], partsB[2], partsB[3]);

                        context.write(new Text(partsA[0]), new Text(str));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(new BloomFilterJoin().run(args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);

        String[] otherArgs = parser.getRemainingArgs();

        if (otherArgs.length != 3) {
            printUsage();
        }

        Job job = Job.getInstance(conf, "BloomFilterJoin");
        job.setJarByClass(BloomFilterJoin.class);

        job.addCacheFile(new Path(args[1]).toUri());

        // The first two elements of the args array are the two inputs
        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, StudentJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, ScoreJoinMapperWithBloom.class);

        job.setReducerClass(StudentJoinReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 2;
    }

    private void printUsage() {
        System.err.println("Usage: BloomFilterJoin <students_in> <scores_in> <out>");
        ToolRunner.printGenericCommandUsage(System.err);
        System.exit(2);
    }
}
