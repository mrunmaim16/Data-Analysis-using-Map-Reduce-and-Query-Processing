import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.List;
import java.util.Arrays;
import java.util.Map;       
import java.util.HashMap;  

public class Imdb {

    public static class MovieMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Map<String, String[]> genrePeriod = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            genrePeriod.put("Action,Thriller", new String[]{"1991-2000", "2001-2010", "2011-2020"});
            genrePeriod.put("Adventure,Drama", new String[]{"1991-2000", "2001-2010", "2011-2020"});
            genrePeriod.put("Comedy,Romance", new String[]{"1991-2000", "2001-2010", "2011-2020"});
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] parts = value.toString().split(";");
    if (parts.length <= 6) {
        String year = parts[3].trim();
        String rating = parts[4].trim();
        String genres = parts[5].trim();
        String titleType = parts[1].trim();

        // Correct variable name in the condition
        if (!year.equals("\\N") && !rating.equals("\\N")) {
            try {
                int yearNum = Integer.parseInt(year);
                float ratings = Float.parseFloat(rating);  

                // Use the parsed 'ratings' float for comparison
                if (ratings >= 7.5) {
                    if (titleType.equals("movie")) {  // Use equals method for string comparison
                        for (Map.Entry<String, String[]> entry : genrePeriod.entrySet()) {
                            String[] periods = entry.getValue();
                            String[] requiredGenres = entry.getKey().split(",");
                            List<String> movieGenres = Arrays.asList(genres.split(","));

                            for (String period : periods) {
                                String[] years = period.split("-");
                                int startYear = Integer.parseInt(years[0]);
                                int endYear = Integer.parseInt(years[1]);

                                if (yearNum >= startYear && yearNum <= endYear && containsAllGenres(movieGenres, requiredGenres)) {
                                    //String genreKey = entry.getKey().replace(",", ";");
                                    word.set("[" + period + "]," + entry.getKey().replace(",", ";"));
                                    context.write(word, one);
                                }
                            }
                        }
                    }
                }
            } catch (NumberFormatException e) {
                // Handle malformed numbers gracefully
                System.err.println("Skipping record with malformed year or rating: " + value.toString());
            }
        }
    }
}
    private boolean containsAllGenres(List<String> movieGenres, String[] requiredGenres) {
        return Arrays.stream(requiredGenres).allMatch(movieGenres::contains);
    }
}

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        //result.set(sum);
        Text keyFinal = new Text(key.toString() + ",");
        String result = keyFinal.toString() + sum;
        //context.write(keyFinal, result);
        context.write(new Text(result), null);
    }
}


    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Movie Rating Count");
    job.setJarByClass(Imdb.class);
    job.setMapperClass(MovieMapper.class);
    job.setReducerClass(IntSumReducer.class); // Make sure this matches the class name
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

