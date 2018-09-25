import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedCacheJoin {

    public static class MapperMapSideJoinDCacheTextFile extends Mapper<LongWritable, Text, Text, Text> {

    	private static HashMap<String, String> ScoreMap = new HashMap<String, String>();
    	private BufferedReader brReader;
    	private String scores = "";
    	private Text txtMapOutputKey = new Text("");
    	private Text txtMapOutputValue = new Text("");

    	enum MYCOUNTER {
    		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
    	}

    	@Override
    	protected void setup(Context context) throws IOException, InterruptedException {

    		URI[] cacheFilesLocal = context.getCacheFiles();

    		for (URI eachPath : cacheFilesLocal) {
                context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
                loadScoresHashMap(eachPath, context);
    		}
    	}

    	private void loadScoresHashMap(URI filePath, Context context) throws IOException {

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

    				ScoreMap.put(deptFieldArray[0].trim(), deptFieldArray[1].trim() + "\t" + deptFieldArray[2].trim() + "\t" + deptFieldArray[3].trim());
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
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    		context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);

    		if (value.toString().length() > 0) {
    			String arrStudentAttributes[] = value.toString().split(",");

                if (Integer.parseInt(arrStudentAttributes[2]) <= 1989) {
                    return;
                }

    			try {
    				scores = ScoreMap.get(arrStudentAttributes[0].toString());
    			} finally {
    				scores = ((scores == null || scores.equals(null) || scores.equals("")) ? "NOT-FOUND" : scores);
    			}

                // Don't write if no matches were found
                if (scores.equals("NOT-FOUND")) {
                    return;
                }

    			txtMapOutputKey.set(arrStudentAttributes[0].toString());

    			txtMapOutputValue.set(arrStudentAttributes[1].toString() + "\t"	+ arrStudentAttributes[2].toString() + "\t" + scores);

    		}
    		context.write(txtMapOutputKey, txtMapOutputValue);
    		scores = "";
    	}
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
    System.out.printf("Three parameters are required- <input dir> <output dir>\n");
    System.exit(1);
    }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Map-side join with text lookup file in DCache");
        job.addCacheFile(new Path(args[1]).toUri());

    job.setJarByClass(DistributedCacheJoin.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setMapperClass(MapperMapSideJoinDCacheTextFile.class);

    job.setNumReduceTasks(0);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
