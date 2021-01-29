import org.apache.commons.lang.math.FloatRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ThirdQuery {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table movieTable = connection.getTable(TableName.valueOf("movies_table"));
        List<Double> years = new ArrayList<>();
        for(double i = 1950.0; i <= 2020.0; i++) {
            years.add(i);
        }
        for (Double year : years) {
            Scan scan = new Scan();
            scan.setReversed(true);
            scan.setStartRow(Bytes.toBytes(year+"+X.X"));
            scan.setStopRow(Bytes.toBytes(year));
            scan.setFilter(new PageFilter(1));
            ResultScanner resultScanner = movieTable.getScanner(scan);
            for (Result res : resultScanner) {
                //Print movie name
                System.out.println(
                        Bytes.toString(res.getValue(Bytes.toBytes("movie_info"),
                                Bytes.toBytes("movie_name"))));

                System.out.println(
                        Bytes.toString(res.getValue(Bytes.toBytes("movie_info"),
                                Bytes.toBytes("director"))));


            }
        }

    }
}
