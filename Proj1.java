import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * This is the skeleton for CS61c project 1, Fall 2012.
 *
 * Contact Alan Christopher or Ravi Punj with questions and comments.
 *
 * Reminder:  DO NOT SHARE CODE OR ALLOW ANOTHER STUDENT TO READ YOURS.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */
public class Proj1 {

    /** An Example Writable which contains two String Objects. */
    public static class StringPair implements Writable {
        /** The String objects I wrap. */
		private String a, b;

		/** Initializes me to contain empty strings. */
		public StringPair() {
			a = b = "";
		}
	
		/** Initializes me to contain A, B. */
        public StringPair(String a, String b) {
            this.a = a;
			this.b = b;
        }

        /** Serializes object - needed for Writable. */
        public void write(DataOutput out) throws IOException {
            new Text(a).write(out);
			new Text(b).write(out);
        }

        /** Deserializes object - needed for Writable. */
        public void readFields(DataInput in) throws IOException {
			Text tmp = new Text();
			tmp.readFields(in);
			a = tmp.toString();
			
			tmp.readFields(in);
			b = tmp.toString();
        }

		/** Returns A. */
		public String getA() {
			return a;
		}
		/** Returns B. */
		public String getB() {
			return b;
		}
    }


  /**
   * Inputs a set of (docID, document contents) pairs.
   * Outputs a set of (Text, Text) pairs.
   */
    public static class Map1 extends Mapper<WritableComparable, Text, Text, Text> {
        /** Regex pattern to find words (alphanumeric + _). */
        final static Pattern WORD_PATTERN = Pattern.compile("\\w+");

        private String targetGram = null;
		private int funcNum = 0;

        /*
         * Setup gets called exactly once for each mapper, before map() gets called the first time.
         * It's a good place to do configuration or setup that can be shared across many calls to map
         */
        @Override
        public void setup(Context context) {
            targetGram = context.getConfiguration().get("targetGram").toLowerCase();
			try {
				funcNum = Integer.parseInt(context.getConfiguration().get("funcNum"));
			} catch (NumberFormatException e) {
				/* Do nothing. */
			}
        }

        @Override
        public void map(WritableComparable docID, Text docContents, Context context)
                throws IOException, InterruptedException {
            Matcher matcher = WORD_PATTERN.matcher(docContents.toString());
			Func func = funcFromNum(funcNum);
			
			ArrayList<Integer> targetPositions = new ArrayList<Integer>();
			String content = docContents.toString().toLowerCase();
			String[] words = content.split(" ");
			int i, j, index, tmp;
			int n = targetGram.split(" ").length;
			double distance;
			String ngram;
			
			while (true) {
				index = content.indexOf(targetGram, index);
				if (index==0) break;
				targetPositions.append(new Integer(index));
			}

			for (i=0; i<content.length-n; i++){
				for (j=0; j<targetPositions.size(); j++){
					distance = Double.POSITIVE_INFINITY;
					tmp = Math.abs(targetPositions.get(j).inValue()-i);
					if (distance>tmp) {
						distance = tmp;
					}
				}
				ngram = "";
				for (j=i; j<i+n; j++){
					ngram = ngram + word[j];
				}
				if (!ngram.equals(targetGram)){
					context.write(new Text(ngram), new Text(""+func(distance)));
				}
			}
		}

		/** Returns the Func corresponding to FUNCNUM*/
		private Func funcFromNum(int funcNum) {
			Func func = null;
			switch (funcNum) {
				case 0:	
					func = new Func() {
						public double f(double d) {
							return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0;
						}			
					};	
					break;
				case 1:
					func = new Func() {
						public double f(double d) {
							return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0 + 1.0 / d;
						}			
					};
					break;
				case 2:
					func = new Func() {
						public double f(double d) {
							return d == Double.POSITIVE_INFINITY ? 0.0 : Math.sqrt(d);
						}			
					};
					break;
			}
			return func;
		}
    }

    /** Here's where you'll be implementing your combiner. It must be non-trivial for you to receive credit. */
    public static class Combine1 extends Reducer<Text, Text, Text, Text> {

      @Override
      public void reduce(Text key, Iterable<Text> values,
              Context context) throws IOException, InterruptedException {
	  
      }
    }


    public static class Reduce1 extends Reducer<Text, Text, DoubleWritable, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values,
			   Context context) throws IOException, InterruptedException {
			int G = 0;
			double Sg = 0;
			double coRate = 0;
			for (Text value : values){
				Sg += Double.parseDouble(value.toString());
				G++;
			}
			if (Sg>0){
				coRate = Sg*Math.pow(Math.log(Sg),3)/G;
			}
			context.write(new DoubleWritable(coRate), key);
        }
    }

    public static class Map2 extends Mapper<DoubleWritable, Text, DoubleWritable, Text> {
	//maybe do something, maybe don't
    }

    public static class Reduce2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

      int n = 0;
      static int N_TO_OUTPUT = 100;

      /*
       * Setup gets called exactly once for each reducer, before reduce() gets called the first time.
       * It's a good place to do configuration or setup that can be shared across many calls to reduce
       */
      @Override
      protected void setup(Context c) {
        n = 0;
      }

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
	    //you should be outputting the final values here.
        }
    }

    /*
     *  You shouldn't need to modify this function much. If you think you have a good reason to,
     *  you might want to discuss with staff.
     *
     *  The skeleton supports several options.
     *  if you set runJob2 to false, only the first job will run and output will be
     *  in TextFile format, instead of SequenceFile. This is intended as a debugging aid.
     *
     *  If you set combiner to false, neither combiner will run. This is also
     *  intended as a debugging aid. Turning on and off the combiner shouldn't alter
     *  your results. Since the framework doesn't make promises about when it'll
     *  invoke combiners, it's an error to assume anything about how many times
     *  values will be combined.
     */
    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        boolean runJob2 = conf.getBoolean("runJob2", true);
        boolean combiner = conf.getBoolean("combiner", false);

        if(runJob2)
          System.out.println("running both jobs");
        else
          System.out.println("for debugging, only running job 1");

        if(combiner)
          System.out.println("using combiner");
        else
          System.out.println("NOT using combiner");

        Path inputPath = new Path(args[0]);
        Path middleOut = new Path(args[1]);
        Path finalOut = new Path(args[2]);
        FileSystem hdfs = middleOut.getFileSystem(conf);
        int reduceCount = conf.getInt("reduces", 32);

        if(hdfs.exists(middleOut)) {
          System.err.println("can't run: " + middleOut.toUri().toString() + " already exists");
          System.exit(1);
        }
        if(finalOut.getFileSystem(conf).exists(finalOut) ) {
          System.err.println("can't run: " + finalOut.toUri().toString() + " already exists");
          System.exit(1);
        }

        {
            Job firstJob = new Job(conf, "wordcount+co-occur");

            firstJob.setJarByClass(Map1.class);

	    /* You may need to change things here */
            firstJob.setMapOutputKeyClass(Text.class);
            firstJob.setMapOutputValueClass(Text.class);
            firstJob.setOutputKeyClass(DoubleWritable.class);
            firstJob.setOutputValueClass(Text.class);
	    /* End region where we expect you to perhaps need to change things. */

            firstJob.setMapperClass(Map1.class);
            firstJob.setReducerClass(Reduce1.class);
            firstJob.setNumReduceTasks(reduceCount);


            if(combiner)
              firstJob.setCombinerClass(Combine1.class);

            firstJob.setInputFormatClass(SequenceFileInputFormat.class);
            if(runJob2)
              firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(firstJob, inputPath);
            FileOutputFormat.setOutputPath(firstJob, middleOut);

            firstJob.waitForCompletion(true);
        }

        if(runJob2) {
            Job secondJob = new Job(conf, "sort");

            secondJob.setJarByClass(Map1.class);
	    /* You may need to change things here */
            secondJob.setMapOutputKeyClass(DoubleWritable.class);
            secondJob.setMapOutputValueClass(Text.class);
            secondJob.setOutputKeyClass(DoubleWritable.class);
            secondJob.setOutputValueClass(Text.class);
	    /* End region where we expect you to perhaps need to change things. */

            secondJob.setMapperClass(Map2.class);
            if(combiner)
              secondJob.setCombinerClass(Reduce2.class);
            secondJob.setReducerClass(Reduce2.class);

            secondJob.setInputFormatClass(SequenceFileInputFormat.class);
            secondJob.setOutputFormatClass(TextOutputFormat.class);
            secondJob.setNumReduceTasks(1);


            FileInputFormat.addInputPath(secondJob, middleOut);
            FileOutputFormat.setOutputPath(secondJob, finalOut);

            secondJob.waitForCompletion(true);
        }
    }

}
