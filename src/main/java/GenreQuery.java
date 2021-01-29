import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class GenreQuery {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
//Get connection instance from configuration
        Connection connection = ConnectionFactory.createConnection(config);
//Get instances from all tables
        Table movieTable = connection.getTable(TableName.valueOf("movies_table"));
        Scan scan = new Scan();
//Set Prefix
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("name_genre"), CompareFilter.CompareOp.valueOf("EQUAL"),  new BinaryComparator(Bytes.toBytes("Comedy")));
        scan.setFilter(filter);
        ResultScanner resultScanner = movieTable.getScanner(scan);
//Iterate result
        for (Result res : resultScanner) {
            //Print movie name
            System.out.print(
                    Bytes.toString(res.getValue(Bytes.toBytes("cf"),
                            Bytes.toBytes("title"))));
            System.out.print(
                    Bytes.toString(res.getValue(Bytes.toBytes("cf"),
                            Bytes.toBytes("name_genre"))));
            System.out.println("");
        }
    }
}
