package lab3;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
 * spark-2.4.5 与 hbase-2.2.4 的jar包冲突
 *
 * 1. netty-all 需要向高版本对齐
 *   [hbase] netty-all-4.0.23.Final.jar --> [spark] netty-all-4.1.42.Final.jar
 *
 * 2. jackson-* 需要向低版本对齐
 *   [hbase] jackson-annotations-2.9.10.jar              --> [spark] jackson-annotations-2.6.7.jar
 *   [hbase] jackson-core-2.9.10.jar                     --> [spark] jackson-core-2.6.7.jar
 *   [hbase] jackson-databind-2.9.10.1.jar               --> [spark] jackson-databind-2.6.7.3.jar
 *   [hbase] jackson-module-jaxb-annotations-2.9.10.jar  --> [spark] jackson-module-jaxb-annotations-2.6.7.jar
 *
 * */

public class PageRank_Spark {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.exit(1);
        }
        final String input = args[0];
        final String output = args[1];
        final int max_iteration = args.length >= 3 ? Integer.parseInt(args[2]) : 10;
        final double d = args.length >= 4 ? Double.parseDouble(args[3]) : 0.85;
        final double PR_init = args.length >= 5 ? Double.parseDouble(args[4]) : 1.0;

        SparkConf config = new SparkConf().setAppName("PageRank_Spark");
        config.setMaster("local[*]");
        JavaSparkContext spark = new JavaSparkContext(config);

        /*
        用到的数据结构:
            两个rdd:
                links: 记录拓扑结构, 邻接表  key 点 value link
                ranks: 当前每个url的pagerank
            一个临时rdd
                contributes: 记录每个url所得到的贡献值, 一轮迭代结束以后做 reduce, 更新ranks

        * 1. 构造邻接表, 初始化pagerank
        *   按 '\t' 分割字符串, 前者是source, 后者是链接出的url, 按照逗号分割可以得到links
        * 2. 迭代计算每个点的pagerank
        *   邻接表和page表做"自然连接" (join)
        *
        * 3. 统计最终的结果,按pagerank从大到小按格式输出
        *   先将 key value 交换, 按照key降序排序, 再将 key value 交换, 按格式输出
        *
         */
        JavaRDD<String> lines = spark.textFile(input);

        // 按\t分割得到url和neighbor url,再把neighbor url按,分割
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
            String[] split = s.split("\t");
            return new Tuple2<>(split[0], Arrays.asList(split[1].split(",")));
        });
        // 初始化rank为1.0
        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> PR_init);


        for (int i = 0; i < max_iteration; ++i) {
            JavaPairRDD<String, Double> contributes =
                    links.join(ranks).values().flatMapToPair(s -> {         // tuple<>
                        // 邻接表的大小
                        int size = Iterables.size(s._1());
                        List<Tuple2<String, Double>> ret = new ArrayList<>();
                        for (String v : s._1()) {
                            // 对于每个v都有 PR/size 的贡献
                            ret.add(new Tuple2<>(v, s._2() / size));
                        }
                        return ret.iterator();
                    });
            // 先累个和sum然后套公式 sum*d+(1-d)
            ranks = contributes.reduceByKey(Double::sum).mapValues(rk -> rk * d + (1 - d));
        }
        // 把<key,value>交换一下按照key降序排序
        JavaRDD<String> ret = ranks.mapToPair(s -> new Tuple2<>(s._2(), s._1()))
                .sortByKey(false)
                .map(s -> String.format("(%s,%.10f)", s._2(), s._1()));
        // 把rdd整合到一个分区然后保存到文件里
        ret.coalesce(1, true).saveAsTextFile(output);
        spark.stop();
    }
}
