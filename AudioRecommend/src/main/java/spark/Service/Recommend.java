package spark.Service;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.lit;


@Component
public class Recommend {
    private final String BASE_PATH = "hdfs://localhost:9000/user/jeshrz/";
    private final String DATA_PATH = BASE_PATH + "lab5/data/";
    private final String ARTIST_ALIAS = DATA_PATH + "artist_alias.txt";
    private final String ARTIST_TXT = DATA_PATH + "artist_data.txt";
    private final String USER_TXT = DATA_PATH + "user_artist_data.txt";

//    private final Path DATA_PATH = Paths.get("lab5/data");
//    private final Path ARTIST_ALIAS = DATA_PATH.resolve("artist_alias.txt");
//    private final Path ARTIST_TXT = DATA_PATH.resolve("artist_data.txt");
//    private final Path USER_TXT = DATA_PATH.resolve("user_artist_data.txt");

    private SparkSession sc;
    private Dataset<Row> userData;
    private Dataset<Row> artistId;
    private ALSModel model;

    public static class Rating implements Serializable {
        private int user;
        private int artist;
        private int count;

        public Rating() {
        }

        public Rating(int user, int artist, int count) {
            this.user = user;
            this.artist = artist;
            this.count = count;
        }

        public int getUser() {
            return user;
        }

        public int getArtist() {
            return artist;
        }

        public int getCount() {
            return count;
        }

        public static Rating parseRating(Row row) {
            String[] split = row.toString().split(",");
            if (split.length != 3) {
                throw new IllegalArgumentException("Each line mush contain 3 fields");
            }
            int artist = Integer.parseInt(split[0].substring(1));
            int count = Integer.parseInt(split[1]);
            int user = Integer.parseInt(split[2].substring(0, split[2].length() - 1));
            return new Rating(user, artist, count);
        }
    }

    public static class Artist implements Serializable {
        private int artist;
        private String name;

        public Artist() {
        }

        public Artist(int artist, String name) {
            this.artist = artist;
            this.name = name;
        }

        public int getArtist() {
            return artist;
        }

        public String getName() {
            return name;
        }

        public static Artist parseArtist(Row row) {
            String[] split = row.toString().split(",");
            if (split.length != 2) {
                throw new IllegalArgumentException("Each line mush contain 3 fields");
            }
            int artist = Integer.parseInt(split[0].substring(1));
            String name = split[1].substring(0, split[1].length() - 1);
            return new Artist(artist, name);
        }
    }

    private SparkSession getSparkSession() {
        return SparkSession.builder().appName("AudioRecommend").master("local[*]").getOrCreate();
    }

    public Recommend() {
        this.sc = getSparkSession();
        SparkContext.getOrCreate().setLogLevel("WARN");
        sc.conf().set("spark.sql.crossJoin.enabled", "true");
        sc.conf().set("spark.driver.memory", "2g");
        sc.conf().set("spark.executor-memory", "2g");
    }

    public Recommend loadData() {
        JavaRDD<String> rawArtistAlias = sc.read().textFile(ARTIST_ALIAS.toString()).javaRDD();
        JavaRDD<String> rawArtistData = sc.read().textFile(ARTIST_TXT.toString()).javaRDD();
        JavaRDD<String> rawUserData = sc.read().textFile(USER_TXT.toString()).javaRDD();

        List<Tuple2<Integer, Integer>> artistAlias = rawArtistAlias.map(line -> {
            String[] split = line.split("\t");
            if (!split[0].equals("")) return new Tuple2<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
            return new Tuple2<>(-1, -1);
        }).filter(pair -> pair._1() != -1).collect();
        Map<Integer, Integer> bArtistAlias = sc.sparkContext().broadcast(artistAlias, scala.reflect.ClassTag$.MODULE$.apply(List.class))
                .value()
                .stream()
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

        JavaRDD<Artist> tmpArtistIdRDD = rawArtistData.map(line -> {
            String[] split = line.split("\t");
            try {
                return new Artist(Integer.parseInt(split[0]), split[1].trim());
            } catch (Exception e) {
                return new Artist(-1, "");
            }
        }).filter(pair -> pair.getArtist() != -1);
        artistId = sc.createDataFrame(tmpArtistIdRDD, Artist.class);

        JavaRDD<Rating> tmpUserDataRDD = rawUserData.map(line -> {
            String[] split = line.split(" ");
            int user = Integer.parseInt(split[0]);
            int artist = Integer.parseInt(split[1]);
            int count = Integer.parseInt(split[2]);
            artist = bArtistAlias.getOrDefault(artist, artist);
            return new Rating(user, artist, count);
        });
        userData = sc.createDataFrame(tmpUserDataRDD, Rating.class);
        return this;
    }

    public Recommend fit() {
        model = new ALS()
                .setSeed(new Random().nextLong())
                .setImplicitPrefs(true)
                .setRank(10)
                .setRegParam(0.01)
                .setAlpha(1.0)
                .setMaxIter(5)
                .setUserCol("user")
                .setItemCol("artist")
                .setRatingCol("count")
                .setPredictionCol("prediction")
                .fit(userData.randomSplit(new double[]{0.9, 0.1})[0]);
        return this;
    }

    public Recommend fit(String saveTo) throws IOException {
        model = new ALS()
                .setSeed(new Random().nextLong())
                .setImplicitPrefs(true)
                .setRank(10)
                .setRegParam(0.01)
                .setAlpha(1.0)
                .setMaxIter(5)
                .setUserCol("user")
                .setItemCol("artist")
                .setRatingCol("count")
                .setPredictionCol("prediction")
                .fit(userData.randomSplit(new double[]{0.9, 0.1})[0]);
        model.save(saveTo);
        return this;
    }

    public Recommend loadModel(String loadFrom) {
        model = ALSModel.load(BASE_PATH + loadFrom);
        return this;
    }

    public List<Tuple2<String, Double>> recommend(int user, int topN) {
        Dataset<Row> toRecommend = model.itemFactors().select(new Column("id").as("artist")).withColumn("user", lit(user));
        Dataset<Row> topRecommendations = model.transform(toRecommend)
                .select("artist", "prediction")
                .orderBy(new Column("prediction").desc())
                .limit(topN);
        Dataset<Row> recommendedArtistIDs = topRecommendations.select("artist", "prediction");
        List<Row> rowOfRecommendedArtistNames = artistId.join(recommendedArtistIDs, "artist")
                .select("name", "prediction").collectAsList();
        List<Tuple2<String, Double>> recommendArtistWithPrediction = new ArrayList<>();
        for (Row row : rowOfRecommendedArtistNames) {
            String[] split = row.toString().split(",");
            String name = split[0].substring(1);
            Double prediction = Double.parseDouble(split[1].substring(0, split[1].length() - 1));
            recommendArtistWithPrediction.add(new Tuple2<>(name, prediction));
        }
        return recommendArtistWithPrediction;
    }
}
