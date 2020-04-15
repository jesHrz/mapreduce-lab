package lab1;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        HDFSTest hdfs = new HDFSTest("hdfs://localhost:9000");
//        hdfs.CopyFromLocal("test/hello.txt", "/test/hello.txt");
//        hdfs.CopyToLocal("/test/hello.txt", "download.txt");
//        hdfs.Cat("/test/hello.txt");
//        hdfs.OneFileStatus("/test/hello.txt");
//        hdfs.AllFileStatus("/");
//        hdfs.CreateFile("/test/hrz.txt");
//        hdfs.CreateDir("/jeshrz");
//        hdfs.DeleteFile("/test/hrz.txt");
//        hdfs.DeleteDir("/jeshrz");
//        hdfs.DeleteFile("/test/hrz.txt");
//        hdfs.AppendFromLocal("test/hello.txt", "test/hrz.txt", 0);
        hdfs.Move("/test/hello.txt", "/test/hrz.txt");
        hdfs.Cat("/test/hello.txt");
        hdfs.Cat("/test/hrz.txt");
    }
}
