import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;
import java.util.stream.Collectors;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Main {
    public static void Q1() {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("How many movies released every year");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = sparkContext.textFile("hdfs://salem.cs.colostate.edu:31190/spark/data/movies.csv");

        JavaPairRDD<Integer, Integer> yearCounts = inputFile.mapToPair( x -> {
            try {
                String s = x.split(",")[1];
                Integer year = Integer.parseInt(s.substring(s.lastIndexOf("(") + 1, s.lastIndexOf(")")));
                return new Tuple2<Integer, Integer>(year, 1);
            } catch (Exception e) {
                return new Tuple2<>(0, 1);
            }
        });
        JavaPairRDD<Integer, Integer> yearCountsSum = yearCounts.reduceByKey((x, y) -> (x + y));
        JavaPairRDD<Integer, Integer> swapped = yearCountsSum.mapToPair(Tuple2::swap);
        JavaPairRDD<Integer, Integer> sorted = swapped.sortByKey();

        sorted.saveAsTextFile("./outputs/Q1");
        sparkContext.close();
        sparkContext.stop();
    }

    public static void Q2() throws IOException {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Average number of genres");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/spark/data/movies.csv");
        String header1 = inputFile.first();

        JavaPairRDD<String, Integer> genreCounts = inputFile.filter(line -> !line.equals(header1)).mapToPair(x -> {
            String[] split = x.split(",")[2].split("\\|");
            return new Tuple2<>(x.split(",")[0], split.length);
        });
        JavaRDD<Integer> values = genreCounts.values();
        Double average = values.reduce((x, y) -> (x + y)) / Double.valueOf(values.count());
        
        genreCounts.saveAsTextFile("./outputs/Q2");
        FileWriter writer = new FileWriter("./outputs/Q2/count.txt", true);
        writer.write("Average: " + average);
        
        writer.close();
        sc.close();
        sc.stop();
    }

    public static void Q3() {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Genres in order of ratings");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> moviesFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/spark/data/movies.csv");
        JavaRDD<String> ratingsFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/spark/data/ratings.csv");

        String header1 = moviesFile.first();
        String header2 = ratingsFile.first();
        JavaRDD<String> moviesNoHeader = moviesFile.filter(line -> !line.equals(header1));
        JavaRDD<String> ratingsNoHeader = ratingsFile.filter(line -> !line.equals(header2));

        JavaPairRDD<Integer, String[]> movies = moviesNoHeader.mapToPair(x -> {
            String[] desc = x.split(",");
            Integer movieID = Integer.parseInt(desc[0]);
            String[] genres = desc[desc.length-1].split("\\|");
            return new Tuple2<>(movieID, genres);
        });
        JavaPairRDD<Integer, Double> ratings = ratingsNoHeader.mapToPair(x -> {
            Integer movieId = Integer.parseInt(x.split(",")[1]);
            Double rating = Double.parseDouble(x.split(",")[2]);
            return new Tuple2<>(movieId, rating);
        });
        JavaPairRDD<Integer, Tuple2<Double, String[]>> joined = ratings.join(movies);
        JavaPairRDD<String, Double> flattened = joined.flatMapToPair(x -> {
            Double key = x._2._1;
            String[] genres = x._2._2;
            return Arrays.stream(genres).map(value -> new Tuple2<>(value, key)).iterator();
        });
        JavaPairRDD<String, Iterable<Double>> groupedByGenre = flattened.groupByKey();
        JavaPairRDD<Double, String> averages = groupedByGenre.mapToPair(x -> {
            Double average = StreamSupport.stream(x._2.spliterator(), false).mapToDouble(Double::doubleValue).average().getAsDouble();
            return new Tuple2<>(average, x._1);
        });
        JavaPairRDD<Double, String> sorted = averages.sortByKey();

        sorted.coalesce(1).saveAsTextFile("./outputs/Q3");
        sc.close();
        sc.stop();
    }

    public static void Q4() {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Top 3 combinations of genres");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> moviesFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/spark/data/movies.csv");
        JavaRDD<String> ratingsFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/spark/data/ratings.csv");

        String header1 = moviesFile.first();
        String header2 = ratingsFile.first();
        JavaRDD<String> moviesNoHeader = moviesFile.filter(line -> !line.equals(header1));
        JavaRDD<String> ratingsNoHeader = ratingsFile.filter(line -> !line.equals(header2));

        JavaPairRDD<Integer, String[]> movies = moviesNoHeader.mapToPair(x -> {
            String[] desc = x.split(",");
            Integer movieID = Integer.parseInt(desc[0]);
            String[] genres = desc[desc.length-1].split("\\|");
            return new Tuple2<>(movieID, genres);
        });
        JavaPairRDD<Integer, String[]> filteredMovies = movies.filter(x -> {
            return x._2.length >= 2;
        });
        JavaPairRDD<Integer, Double> ratings = ratingsNoHeader.mapToPair(x -> {
            Integer movieId = Integer.parseInt(x.split(",")[1]);
            Double rating = Double.parseDouble(x.split(",")[2]);
            return new Tuple2<>(movieId, rating);
        });
        JavaPairRDD<Integer, Tuple2<Double, String[]>> joined = ratings.join(filteredMovies);
        JavaPairRDD<String, Double> flattened = joined.mapToPair(x -> {
            Double rating = x._2._1;
            String genres = Arrays.toString(x._2._2);
            return new Tuple2<>(genres, rating);
        });
        JavaPairRDD<String, Iterable<Double>> groupedByGenre = flattened.groupByKey();
        JavaPairRDD<Double, String> averages = groupedByGenre.mapToPair(x -> {
            Double average = StreamSupport.stream(x._2.spliterator(), false).mapToDouble(Double::doubleValue).average().getAsDouble();
            return new Tuple2<>(average, x._1);
        });
        JavaPairRDD<Double, String> sorted = averages.sortByKey(false);

        sorted.coalesce(1).saveAsTextFile("./outputs/Q4");
        sc.close();
        sc.stop();
    }

    public static void Q5() throws IOException {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Movies with %comedy%");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> moviesFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/spark/data/movies.csv");

        String header1 = moviesFile.first();
        JavaRDD<String> moviesNoHeader = moviesFile.filter(line -> !line.equals(header1));

        JavaPairRDD<Integer, String> movies = moviesNoHeader.mapToPair(x -> {
            String[] desc = x.split(",");
            Integer movieID = Integer.parseInt(desc[0]);
            String genres = Arrays.toString(desc[desc.length-1].split("\\|"));
            return new Tuple2<>(movieID, genres);
        });
        JavaPairRDD<Integer, String> filteredMovies = movies.filter(x -> {
            return (x._2.matches("(?i).*comedy.*"));
        });
        Long comedyCount = filteredMovies.count();

        filteredMovies.saveAsTextFile("./outputs/Q5");
        FileWriter writer = new FileWriter("./outputs/Q5/count.txt", true);
        writer.append("Count: " + comedyCount);

        writer.close();
        sc.close();
        sc.stop();
    }

    public static void Q6() {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Movies released per genre");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> moviesFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/spark/data/movies.csv");

        String header1 = moviesFile.first();
        JavaRDD<String> moviesNoHeader = moviesFile.filter(line -> !line.equals(header1));

        JavaPairRDD<Integer, String[]> movies = moviesNoHeader.mapToPair(x -> {
            String[] desc = x.split(",");
            Integer movieID = Integer.parseInt(desc[0]);
            String[] genres = desc[desc.length-1].split("\\|");
            return new Tuple2<>(movieID, genres);
        });
        JavaPairRDD<String, Integer> flattened = movies.flatMapToPair(x -> {
            String[] genres = x._2;
            return Arrays.stream(genres).map(value -> new Tuple2<>(value, 1)).iterator();
        });
        JavaPairRDD<String, Integer> reducedByGenre = flattened.reduceByKey((x, y) -> (x + y));
        JavaPairRDD<Integer, String> swapped = reducedByGenre.mapToPair(Tuple2::swap);
        JavaPairRDD<Integer, String> sorted = swapped.sortByKey(false);

        sorted.saveAsTextFile("./outputs/Q6");
        sc.close();
        sc.stop();
    }

    public static void Q7() {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Q7");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> genomeTags = sc.textFile("hdfs://salem.cs.colostate.edu:31190/spark/data/genome-tags.csv");
        JavaRDD<String> genomeScores = sc.textFile("hdfs://salem.cs.colostate.edu:31190/spark/data/genome-scores.csv");
        JavaRDD<String> movies = sc.textFile("hdfs://salem.cs.colostate.edu:31190/spark/data/movies.csv");

        String header = genomeTags.first();
        String header2 = genomeScores.first();
        String header3 = movies.first();

        JavaPairRDD<Integer, String> scoresRDD = genomeScores.filter(line -> !line.equals(header2)).mapToPair(x -> {
            String[] desc = x.split(",");
            Integer key = Integer.parseInt(desc[1]);
            String rest = desc[0] + "," + desc[2];
            return new Tuple2<>(key, rest);
        });
        JavaPairRDD<Integer, String> genomeTagsRDD = genomeTags.filter(line -> !line.equals(header)).mapToPair(x -> {
            String[] desc = x.split(",");
            Integer key = Integer.parseInt(desc[0]);
            return new Tuple2<>(key, desc[1]);
        });
        JavaPairRDD<Integer, String> moviesRDD = movies.filter(line -> !line.equals(header3)).mapToPair(x -> {
            String lastRemoved = x.substring(0, x.lastIndexOf(","));
            String[] str = lastRemoved.split(",", 2);
            return new Tuple2<>(Integer.parseInt(str[0]), str[1]);
        });

        JavaPairRDD<Integer,Iterable<Tuple2<Double,String>>> top5Tags = genomeTagsRDD.join(scoresRDD)
            .mapToPair(x -> {
                Integer movieID = Integer.parseInt(x._2._2.split(",")[0]);
                String tag = x._2._1;
                Double relevance = Double.parseDouble(x._2._2.split(",")[1]);
                return new Tuple2<>(movieID, new Tuple2<>(relevance, tag));
            }).groupByKey().mapValues(i -> {
                List<Tuple2<Double, String>> list = StreamSupport.stream(i.spliterator(), false).collect(Collectors.toList());
                list.sort(Comparator.comparing((Tuple2<Double, String> tuple) -> tuple._1).reversed());
                return list.subList(0, Math.min(5, list.size()));
            });
        top5Tags.coalesce(1).saveAsTextFile("./outputs/Q7/top5GenomeTags");
        
        JavaPairRDD<String, String> movieTags = moviesRDD.join(top5Tags).mapToPair(x -> {
            String movieName = x._2._1;
            List<Tuple2<Double, String>> list = StreamSupport.stream(x._2._2.spliterator(), false).collect(Collectors.toList());
            String values = String.join(", ", list.stream().map(Tuple2::_2).collect(Collectors.toList()));
            return new Tuple2<>(movieName, values);
        });
        movieTags.coalesce(1).saveAsTextFile("./outputs/Q7/finalOutput");

        sc.close();
        sc.stop();
    }

    public static void main(String[] args) throws IOException {
        Q1();
        Q2();
        Q3();
        Q4();
        Q5();
        Q6();
        Q7();
    }
}