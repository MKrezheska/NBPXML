import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ActorsQuery {
    public static void main(String[] args) throws IOException {
        double total = 0;
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table movieTable = connection.getTable(TableName.valueOf("actors_table"));
        List<Long> actors = new ArrayList<>();
        for (long i = 0; i <= 206157; i++) {
            actors.add(i);
        }
        for (Long actor : actors) {
            Scan scan = new Scan();
            scan.setReversed(true);
            scan.setStartRow(Bytes.toBytes(actor + "+X.X"));
            scan.setStopRow(Bytes.toBytes(actor.toString()));
            scan.setFilter(new PageFilter(1));
            long startTime = System.nanoTime();
            ResultScanner resultScanner = movieTable.getScanner(scan);
            long elapsedTime = System.nanoTime() - startTime;
            total += elapsedTime;

            for (Result res : resultScanner) {
                System.out.print(
                        Bytes.toString(res.getValue(Bytes.toBytes("actors"),
                                Bytes.toBytes("character"))) + ", ");
                System.out.print(
                        Bytes.toString(res.getValue(Bytes.toBytes("actors"),
                                Bytes.toBytes("name_actor"))) + ", ");
                System.out.print(
                        Bytes.toString(res.getValue(Bytes.toBytes("actors"),
                                Bytes.toBytes("title"))) + ", ");
                System.out.println(
                        Bytes.toString(res.getValue(Bytes.toBytes("actors"),
                                Bytes.toBytes("rating"))));

            }


        }
        System.out.println("Total execution time in seconds: "
                + total / 1000000000.0);

    }
}

