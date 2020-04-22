package lab1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HBaseTest {
    private Connection connection = null;
    private Configuration config = null;

    /*建立连接*/
    public void Connect() throws IOException {
        config = HBaseConfiguration.create();
        config.set("log4j.logger.org.apache.hadoop.hbase", "WARN");
        config.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        connection = ConnectionFactory.createConnection(config);
        System.out.println("connected");
    }

    /*关闭连接*/
    public void Close() throws IOException {
        connection.close();
    }

    /*列出 HBase 所有的表的相关信息，例如表名*/
    /*shell: list*/
    public void ListTables() throws IOException {
        Admin admin = connection.getAdmin();
        for (HTableDescriptor descriptor : admin.listTables()) {
            System.out.println(descriptor.getNameAsString());
        }
    }

    /*在终端打印出指定的表的所有记录数据*/
    /*shell: scan "<tableName>"*/
    public void ScanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result ret : scanner) {
            for (Cell cell : ret.rawCells()) {
                System.out.println("Row: " + new String(CellUtil.cloneRow(cell)));
                System.out.println("Timestamp: " + cell.getTimestamp());
                System.out.println("ColumnFamily: " + new String(CellUtil.cloneFamily(cell)));
                System.out.println("ColumnName: " + new String(CellUtil.cloneQualifier(cell)));
                System.out.println("value: " + new String(CellUtil.cloneValue(cell)));
                System.out.println();
            }
        }
        table.close();
    }

    /*向已经创建好的表添加和删除指定的列族或列*/
    /*创建 shell: alter "<tableName>", "NAME"=>"<column>"*/
    /*删除 shell: alter "<tableName>", "NAME"=>"<column>, METHOD=>"delete"*/
    public void DeleteColumn(String tableName, String column) throws IOException {
        connection.getAdmin().deleteColumnFamily(TableName.valueOf(tableName), column.getBytes());
    }

    public void AddColumn(String tableName, String column) throws IOException {
        connection.getAdmin().addColumnFamily(TableName.valueOf(tableName), new HColumnDescriptor(column));
    }

    /*清空指定的表的所有记录数据*/
    /*shell:
        disable "<tableName>"
        drop "<tableName>"
    */
    public void Drop(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
//        System.out.println("233");
        admin.createTable(descriptor);
    }

    /*统计表的行数*/
    /*shell: count "<tableName>"*/
    public int Count(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(new Scan());
        int cnt = 0;
        for (Result ret : scanner) {
            cnt++;
        }
        table.close();
        return cnt;
    }

    /*创建表，参数 tableName 为表的名称，字符串数组 fields 为存储记录各个字段名称的数组。 要求当
      HBase 已经存在名为 tableName 的表的时候，先删除原有的表，然后再创建新的表。*/
    public void CreateTable(String tableName, String[] fields) throws IOException {
        Admin admin = connection.getAdmin();
        TableName _tableName = TableName.valueOf(tableName);
        if (admin.tableExists(_tableName)) {
            System.out.printf("Table \"%s\" already exists\n", tableName);
            admin.disableTable(_tableName);
            admin.deleteTable(_tableName);
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(_tableName);
        for (String columns : fields) {
            tableDescriptor.addFamily(new HColumnDescriptor(columns));
        }
        admin.createTable(tableDescriptor);
    }

    /*向表 tableName、行 row（用 S_Name 表示）和字符串数组 fields 指定的单元格中添加对应的数据 values 。
    fields 中每个元素如果对应的列族下还有相应的列限定符的话， 用 "columnFamily:column"表示。*/
    public void AddRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (int i = 0; i < fields.length; ++i) {
            Put put = new Put(row.getBytes());
            String[] columns = fields[i].split(":");
            if (columns.length == 1) {
                put.addColumn(columns[0].getBytes(), "".getBytes(), values[i].getBytes());
            } else {
                put.addColumn(columns[0].getBytes(), columns[1].getBytes(), values[i].getBytes());
            }
            table.put(put);
        }
        table.close();
    }

    /*
        浏览表 tableName 某一列的数据。
        要求当参数 column 为某一列族名称时，如果底下有若干个列限定符，则要列出每个列限定符代表的列的数据；
        当参数 column 为某一列具体名称（例如“Score:Math”）时，只需要列出该列的数据。
    */
    public void ScanColumn(String tableName, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(column.getBytes());
        ResultScanner scanner = table.getScanner(scan);
        for (Result ret : scanner) {
            for (Cell cell : ret.rawCells()) {
                System.out.println("Row: " + new String(CellUtil.cloneRow(cell)));
                System.out.println("Timestamp: " + cell.getTimestamp());
                System.out.println("ColumnFamily: " + new String(CellUtil.cloneFamily(cell)));
                System.out.println("ColumnName: " + new String(CellUtil.cloneQualifier(cell)));
                System.out.println("value: " + new String(CellUtil.cloneValue(cell)));
                System.out.println();
            }
        }
        table.close();
    }

    /*修改表 tableName，行 row（可以用学生姓名 S_Name 表示），列 column 指定的单元格的数据*/
    public void ModifyData(String tableName, String row, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(row.getBytes());
        String[] columns = column.split(":");
        if (columns.length == 1) {
            put.addColumn(columns[0].getBytes(), "".getBytes(), value.getBytes());
        } else {
            put.addColumn(columns[0].getBytes(), columns[1].getBytes(), value.getBytes());
        }
        table.put(put);
        table.close();
    }

    /*删除表 tableName 中 row 指定的行的记录*/
    public void DeleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(row.getBytes());
        table.delete(delete);
        table.close();
    }
}
