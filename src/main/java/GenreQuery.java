import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class GenreQuery {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table movieTable = connection.getTable(TableName.valueOf("movies_table"));
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("movie_info"), Bytes.toBytes("name_genre"), CompareFilter.CompareOp.valueOf("EQUAL"),  new BinaryComparator(Bytes.toBytes("Comedy")));
        scan.setFilter(filter);
        long startTime = System.nanoTime();
        ResultScanner resultScanner = movieTable.getScanner(scan);
        long elapsedTime = System.nanoTime() - startTime;

        System.out.println("Total execution time in seconds: "
                + elapsedTime/1000000000.0);

        for (Result res : resultScanner) {
            System.out.print(
                    Bytes.toString(res.getValue(Bytes.toBytes("movie_info"),
                            Bytes.toBytes("title"))) + " ");
            System.out.print(
                    Bytes.toString(res.getValue(Bytes.toBytes("movie_info"),
                            Bytes.toBytes("name_genre"))) + " ");
        }

    }
}
