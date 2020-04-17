package lab1;

import java.io.IOException;

public class Main {
    public static void hdfs_test() throws IOException {
        HDFSTest hdfs = new HDFSTest("hdfs://localhost:9000");
//        hdfs.CopyFromLocal("test/hello.txt", "/jeshrz/hello.txt");
//        hdfs.CopyToLocal("/jeshrz/hello.txt", "download.txt");
//        hdfs.Cat("/test/hello.txt");
//        hdfs.OneFileStatus("/jeshrz/hello.txt");
//        hdfs.AllFileStatus("/");
//        hdfs.CreateFile("test.txt");
//        hdfs.CreateDir("/jeshrz");
//        hdfs.DeleteFile("/test/hrz.txt");
//        hdfs.DeleteDir("/jeshrz");
//        hdfs.DeleteFile("/test/hrz.txt");
        hdfs.Cat("test.txt");
        hdfs.AppendFromLocal("test/hello.txt", "test.txt", 1);
        hdfs.Cat("test.txt");
//        hdfs.Move("/jeshrz/hello.txt", "/jeshrz/hrz.txt");
//        hdfs.Cat("/jeshrz/hello.txt");
//        hdfs.Cat("/jeshrz/hrz.txt");
    }

    public static void hbase_test() throws IOException {
        HBaseTest hbase = new HBaseTest();
        hbase.Connect();
//        hbase.Drop("test");
//        hbase.ListTables();
//        hbase.ScanTable("test");

//        hbase.CreateTable("test", new String[] {"cf1"});
//        hbase.AddColumn("test", "cf2");
//        hbase.DeleteColumn("test", "cf2");
//        hbase.ScanTable("test");
//        hbase.AddRecord("test", "1", new String[] {"cf1", "cf2:c21"}, new String[] {"val1", "val21"});
//        hbase.ScanColumn("test", "cf2");
        hbase.Close();
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        hbase_test();
//        hdfs_test();
//        Mapreduct1_test();
//        MapreduceTest.test1_run();
//        MapreduceTest.test2_run();
        MapreduceTest.test3_run();
    }
}
