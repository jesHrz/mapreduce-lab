package lab1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.Date;
import java.util.Scanner;

public class HDFSTest {
    private FileSystem fileSystem;
    private Configuration conf;
    public HDFSTest(String path) {
        conf = new Configuration();
        conf.set("fs.defaultFS", path);
        conf.setBoolean("dfs.support.append", true);
        conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void finalize() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*向 HDFS 中上传任意文本文件，如果指定的文件在 HDFS 中已经存在，则由用户来指定是追加到原有文件末尾还是覆盖原有的文件*/
    /*测试存在: hadoop fs -test -e <remote>*/
    /*上传shell: hadoop fs -put <local> <remote>*/
    /*覆盖shell: hadoop fs -copyFromLocal <local> <remote>*/
    /*追加末尾shell: hadoop fs -appendToFile <local> <remote>*/
    public void CopyFromLocal(String local, String remote) throws IOException {
        Path localPath = new Path(local);
        Path remotePath = new Path(remote);
        boolean override = false;
        boolean exists = fileSystem.exists(remotePath);
        if (exists) {
            System.out.printf("\"%s\" already exists, override it? ", remote);
            String input = (new Scanner(System.in)).next();
            if (input.charAt(0) == 'y' || input.charAt(0) == 'Y') override = true;
        }
        if(!exists || override) {
            System.out.printf("HDFS %s from local \"%s\" to remote \"%s\"\n", exists ? "override" : "write", local, remote);
            fileSystem.copyFromLocalFile(false, true, localPath, remotePath);
        } else {
            AppendFromLocal(local, remote, 0);
        }
    }

    /*从 HDFS 中下载指定文件，如果本地文件与要下载的文件名称相同，则自动对下载的文件重命名*/
    /*shell: hadoop fs -get <remote> <local>*/
    public void CopyToLocal(String remote, String local) throws IOException {
        System.out.printf("HDFS download remote file \"%s\" to local \"%s\"\n", remote, local);
        for(File localFile = new File(local); localFile.exists(); localFile = new File(local)) {
            int index = local.lastIndexOf('.');
            local = local.substring(0, index) + " new" + local.substring(index);
        }
        fileSystem.copyToLocalFile(new Path(remote), new Path(local));
    }

    /*将 HDFS 中指定文件的内容输出到终端中*/
    /*shell: hadoop fs -cat <remote>*/
    public void Cat(String remote) throws IOException {
        System.out.printf("HDFS cat remote file \"%s\"\n", remote);
        Path remotePath = new Path(remote);
        if(!fileSystem.exists(remotePath)) {
            System.out.printf("%s: No such file or dir\n", remote);
            return;
        }
        if(!fileSystem.getFileStatus(remotePath).isFile()) {
            System.out.printf("%s: Not a file\n", remote);
            return;
        }
        FSDataInputStream contents = fileSystem.open(remotePath);
        BufferedReader buffer = new BufferedReader(new InputStreamReader(contents));
        for(String line = buffer.readLine(); line != null; line = buffer.readLine()) {
            System.out.println(line);
        }
    }

    /*显示 HDFS 中指定的文件的读写权限、大小、创建时间、路径等信息*/
    /*shell: hadoop fs -ls -h <remote>*/
    public void OneFileStatus(String remote) throws IOException {
        System.out.printf("HDFS read file status from remote \"%s\"\n\n", remote);
        Path remotePath = new Path(remote);
        if(!fileSystem.exists(remotePath)) {
            System.out.printf("%s: No such file or dir\n", remote);
            return;
        }
        FileStatus status = fileSystem.getFileStatus(remotePath);
        if(!status.isFile()) {
            System.out.printf("%s: Not a file\n", remote);
            return;
        }

        System.out.printf("*\tPath: %s\n", status.getPath());
        System.out.printf("\tPermission: %s\n", status.getPermission());
        System.out.printf("\tBlock Size: %s bytes\n", status.getBlockSize());
        System.out.printf("\tAccess Time: %s\n", new Date(status.getAccessTime()));
        System.out.println();
    }

    /*给定 HDFS 中某一个目录，递归输出该目录下的所有文件的读写权限、大小、创建时路径等信息*/
    /*shell: hadoop fs -ls -R -h <remote>*/
    public void AllFileStatus(String remote) throws IOException {
        System.out.printf("HDFS read all file status from remote \"%s\"\n\n", remote);
        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(new Path(remote), true);
        while(it.hasNext()) {
            FileStatus status = it.next();
            System.out.printf("*\tPath: %s\n", status.getPath());
            System.out.printf("\tPermission: %s\n", status.getPermission());
            System.out.printf("\tBlock Size: %s bytes\n", status.getBlockSize());
            System.out.printf("\tAccess Time: %s\n", new Date(status.getAccessTime()));
            System.out.println("\n");
        }
    }

    /*在 HDFS 中创建一个文件*/
    /*shell: hadoop fs -touchz <remote>*/
    public void CreateFile(String remote) throws IOException {
        System.out.printf("HDFS create file at remote \"%s\"\n", remote);
        fileSystem.create(new Path(remote));
    }

    /*删除 HDFS 中指定的文件*/
    /*shell: hadoop fs -rm <remote>*/
    public void DeleteFile(String remote) throws IOException {
        System.out.printf("HDFS delete file at remote \"%s\"\n", remote);
        Path remotePath = new Path(remote);
        if(!fileSystem.exists(remotePath)) {
            System.out.printf("%s: No such file\n", remote);
            return;
        }
        if(!fileSystem.getFileStatus(remotePath).isFile()) {
            System.out.printf("%s: Not a file\n", remote);
            return;
        }
        fileSystem.delete(remotePath, false);
    }

    /*在 HDFS 中创建一个目录*/
    /*shell: hadoop fs -mkdir <remote>*/
    public void CreateDir(String remote) throws IOException {
        System.out.printf("HDFS create dir \"%s\"\n", remote);
        fileSystem.mkdirs(new Path(remote));
    }

    /*删除 HDFS 中指定的目录*/
    /*shell: hadoop fs -rmr <remote>*/
    public void DeleteDir(String remote) throws IOException {
        System.out.printf("HDFS delete dir \"%s\"\n", remote);
        Path remotePath = new Path(remote);

        if(!fileSystem.exists(remotePath)) {
            System.out.printf("%s: No such dir\n", remote);
            return;
        }
        if(!fileSystem.getFileStatus(remotePath).isDirectory()) {
            System.out.printf("%s: Not a dir\n", remote);
            return;
        }
        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(remotePath, false);
        if(it.hasNext()) {
            System.out.printf("%s has other files, delete all of them? ", remote);
            String input = (new Scanner(System.in)).next();
            if(input.charAt(0) == 'y' || input.charAt(0) == 'Y')    fileSystem.delete(remotePath, true);
        } else {
            fileSystem.delete(remotePath, false);
        }
    }

    /*向 HDFS 中指定的文件追加内容，由用户指定内容追加到原有文件的开头或结尾*/
    /*shell: hadoop fs -appendToFile <local> <remote>*/
    public void AppendFromLocal(String local, String remote, int pos) throws IOException {
        System.out.printf("HDFS append from local \"%s\" to remote \"%s\"\n", local, remote);

        Path remotePath = new Path(remote);
        Path localPath = new Path(local);

        if(!fileSystem.exists(remotePath)) {
            fileSystem.create(remotePath).close();
        }

        switch(pos) {
            case 0: // 追加到结尾
                FileInputStream in = new FileInputStream(local);
                OutputStream out = fileSystem.append(remotePath);
                IOUtils.copyBytes(in, out, 4096, true);
                break;

            case 1: // 追加到开头
                CopyToLocal(remote, "tmp_" + remote);
                CopyFromLocal(local, remote);
                AppendFromLocal("tmp_" + remote, remote, 0);
                (new File("tmp_" + remote)).delete();
                break;
        }
    }

    /*在 HDFS 中，将文件从源路径移动到目的路径*/
    /*hadoop fs -mv <source> <dest>*/
    public void Move(String remoteSrc, String remoteDest) throws IOException {
        System.out.printf("HDFS move file from remote \"%s\" to remote \"%s\"\n", remoteSrc, remoteDest);
        Path srcPath = new Path(remoteSrc);
        Path destPath = new Path(remoteDest);
        if(!fileSystem.exists(srcPath)) {
            System.out.printf("%s: No such file or dir\n", remoteSrc);
            return;
        }
        if(!fileSystem.getFileStatus(srcPath).isFile()) {
            System.out.printf("%s: Not a file\n", remoteSrc);
            return;
        }
        if(fileSystem.exists(destPath)) {
            System.out.printf("%s: Already exists\n", remoteDest);
            return;
        }
        fileSystem.rename(srcPath, destPath);
    }
}
