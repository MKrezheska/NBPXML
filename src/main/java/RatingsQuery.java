import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RatingsQuery {
    public static void main(String[] args) throws IOException {
        double total = 0;
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table movieTable = connection.getTable(TableName.valueOf("ratings_table"));
        List<Long> users = new ArrayList<>();
        for(long i = 1; i <= 270896; i++) {
            users.add(i);
        }
        for (Long user : users) {
            Scan scan = new Scan();
            scan.setReversed(true);
            scan.setStartRow(Bytes.toBytes(user+"+X.X"));
            scan.setStopRow(Bytes.toBytes(user.toString()));
            scan.setFilter(new PageFilter(1));
            long startTime = System.nanoTime();
            ResultScanner resultScanner = movieTable.getScanner(scan);
            long elapsedTime = System.nanoTime() - startTime;
            total += elapsedTime;

            for (Result res : resultScanner) {
                System.out.print(
                        Bytes.toString(res.getValue(Bytes.toBytes("ratings"),
                                Bytes.toBytes("id_user"))) + " ");
                System.out.println(
                        Bytes.toString(res.getValue(Bytes.toBytes("ratings"),
                                Bytes.toBytes("rating"))) + " ");

            }


        }
        System.out.println("Total execution time in seconds: "
                + total/1000000000.0);

    }
}

/*0.26s*/
