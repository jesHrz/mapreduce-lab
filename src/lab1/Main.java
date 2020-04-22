package lab1;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Main {
    public static void hdfs_test() throws IOException {
        HDFSTest hdfs = new HDFSTest("hdfs://localhost:9000");
//        hdfs.CopyFromLocal("test/hello.txt", "/jeshrz/hello.txt");
//        hdfs.CopyToLocal("/jeshrz/hello.txt", "download.txt");
//        hdfs.Cat("/jeshrz/hello.txt");
//        hdfs.OneFileStatus("/jeshrz/hello.txt");
//        hdfs.AllFileStatus("/jeshrz");
//        hdfs.CreateFile("/jeshrz/dir/test.txt");
//        hdfs.CreateDir("/jeshrz");
//        hdfs.DeleteFile("/jeshrz/notexists.txt");
//        hdfs.DeleteDir("/jeshrz");
//        hdfs.DeleteFile("/test/hrz.txt");
//        hdfs.Cat("test.txt");
//        hdfs.AppendFromLocal("test/hello.txt", "test.txt", 0);
//        hdfs.Cat("test.txt");
//        hdfs.Move("test.txt.bak", "test.txt");
//        hdfs.Cat("/jeshrz/hello.txt");
//        hdfs.Cat("/jeshrz/hrz.txt");
    }

    public static void hbase_test() throws IOException {
        HBaseTest hbase = new HBaseTest();
        hbase.Connect();
//        hbase.ScanTable("test");
//        hbase.Drop("test");
//        hbase.ListTables();
//        hbase.ScanTable("test");
//        System.out.println(hbase.Count("test"));

//        hbase.CreateTable("test", new String[] {"cf1"});
//        hbase.AddColumn("test", "cf2");
//        hbase.DeleteColumn("test", "cf2");
//        hbase.ScanTable("test");
//        hbase.AddRecord("test", "1", new String[] {"cf1", "cf2:c21"}, new String[] {"val1", "val21"});
        hbase.AddRecord("student", "scofield",
                new String[]{"score:English", "score:Math", "score:Computer"}, new String[]{"45", "89", "100"});
//        hbase.ScanColumn("test", "cf2");
        hbase.Close();
    }

    public static void MySQL_test() throws SQLException, ClassNotFoundException {
        final String DRIVER = "com.mysql.cj.jdbc.Driver";
        final String DB = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false";
        final String USER = "root";
        final String PASSWD = "123456";

        Class.forName(DRIVER);
        Connection conn = DriverManager.getConnection(DB, USER, PASSWD);
        Statement stmt = conn.createStatement();
        String sql1 = "insert into student values('scofield',45,89,100)";
        String sql2 = "select name, English from student where name='scofield'";
        stmt.executeUpdate(sql1);
        ResultSet rs = stmt.executeQuery(sql2);
        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getInt(2));
        }
        stmt.close();
        conn.close();
    }

    public static void Redis_test() {
        Jedis jedis = new Jedis("localhost", 6379);
        jedis.hset("student.scofield", "English", "45");
        jedis.hset("student.scofield", "Math", "89");
        jedis.hset("student.scofield", "Computer", "100");
        Map<String, String> rets = jedis.hgetAll("student.scofield");
        for (Map.Entry<String, String> ret : rets.entrySet()) {
            System.out.println(ret.getKey() + "\t" + ret.getValue());
        }
    }

    public static void Mongo_test() {
        MongoClient client = new MongoClient("localhost", 27017);
        MongoCollection<Document> collection = client.getDatabase("student").getCollection("student");
        Document doc = new Document("name", "scofield").append("score", new Document("English", 45).append("Math", 89).append("Computer", 100));
        List<Document> docs = new ArrayList<>();
        docs.add(doc);
        collection.insertMany(docs);

        for (Document document : collection.find(new Document("name", "scofield")).projection(new Document("score", 1).append("_id", 0))) {
            System.out.println(document.toJson());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, SQLException {
//        hbase_test();
//        hdfs_test();
//        Mapreduct1_test();
//        MapreduceTest.test1_run();
//        MapreduceTest.test2_run();
//        MapreduceTest.test3_run();

//        MySQL_test();
//        Redis_test();
        Mongo_test();
    }
}
