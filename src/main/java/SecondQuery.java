import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class SecondQuery {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table movieTable = connection.getTable(TableName.valueOf("movies_table"));
        Scan scan = new Scan();
        FilterList filterList = new FilterList();
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("movie_info"), Bytes.toBytes("id_genre"), CompareFilter.CompareOp.valueOf("EQUAL"),  new BinaryComparator(Bytes.toBytes("35"))));
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("movie_info"), Bytes.toBytes("year"), CompareFilter.CompareOp.valueOf("EQUAL"),  new BinaryComparator(Bytes.toBytes("2005.0"))));
        filterList.addFilter(new PageFilter(10));
        scan.setReversed(true);
        scan.setFilter(filterList);

        long startTime = System.nanoTime();
        ResultScanner resultScanner = movieTable.getScanner(scan);
        long elapsedTime = System.nanoTime() - startTime;

        System.out.println("Total execution time in seconds: "
                + elapsedTime/1000000000.0);

        for (Result res : resultScanner) {
            System.out.println(
                    Bytes.toString(res.getValue(Bytes.toBytes("movie_info"),
                            Bytes.toBytes("title"))) + " " +
                            Bytes.toString(res.getValue(Bytes.toBytes("movie_info"),
                                    Bytes.toBytes("weighted_rating"))) + " " +
                            Bytes.toString(res.getValue(Bytes.toBytes("movie_info"),
                                    Bytes.toBytes("year"))) + " " +
                            Bytes.toString(res.getValue(Bytes.toBytes("movie_info"),
                                    Bytes.toBytes("name_genre"))));
        }
    }
}
