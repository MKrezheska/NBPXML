import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class ActorsReader {
    public static void main(String[] args) throws IOException {
        int CHARACTER = 0;
        int GENDER = 1;
        int NAME_ACTOR = 3;
        int ID_MOVIE = 4;
        int TITLE = 5;
        int RATING = 6;
        int ID_ACTOR = 7;


        //HBase configuration
        Configuration config = HBaseConfiguration.create();
        //Get connection instance from configuration
        Connection connection = ConnectionFactory.createConnection(config);
        //Get movie table instance
        Table ratingsTable = connection.getTable(TableName.valueOf("actors_table"));
        //Read data from file
        int i = 0;
        File file = new File("C:\\Users\\Magdalena\\OneDrive\\Desktop\\archive\\last_kveri.csv");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String row_key = "";
                String[] data = line.split("\\|");

                String actorId = data[ID_ACTOR].replaceAll("[^0-9]", "");
                System.out.println(actorId);

                row_key+= actorId + "+" + data[RATING] + "+" + data[ID_MOVIE];
                Put p = new Put(Bytes.toBytes(row_key));

                p.addColumn(Bytes.toBytes("actors"),
                        Bytes.toBytes("character"),
                        Bytes.toBytes(data[CHARACTER]));
                p.addColumn(Bytes.toBytes("actors"),
                        Bytes.toBytes("gender"),
                        Bytes.toBytes(data[GENDER]));
                p.addColumn(Bytes.toBytes("actors"),
                        Bytes.toBytes("id_actor"),
                        Bytes.toBytes(actorId));
                p.addColumn(Bytes.toBytes("actors"),
                        Bytes.toBytes("name_actor"),
                        Bytes.toBytes(data[NAME_ACTOR]));
                p.addColumn(Bytes.toBytes("actors"),
                        Bytes.toBytes("id_movie"),
                        Bytes.toBytes(data[ID_MOVIE]));
                p.addColumn(Bytes.toBytes("actors"),
                        Bytes.toBytes("title"),
                        Bytes.toBytes(data[TITLE]));
                p.addColumn(Bytes.toBytes("actors"),
                        Bytes.toBytes("rating"),
                        Bytes.toBytes(data[RATING]));

                ratingsTable.put(p);
            }
        }
    }
}
