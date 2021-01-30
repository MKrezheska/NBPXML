import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
public class RatingsReader {
    public static void main( String[] args ) throws IOException {
        int ID_USER = 0;
        int MOVIE_ID = 1;
        int RATING = 2;
        int TIMESTAMP= 3;

        //HBase configuration
        Configuration config = HBaseConfiguration.create();
        //Get connection instance from configuration
        Connection connection = ConnectionFactory.createConnection(config);
        //Get movie table instance
        Table ratingsTable = connection.getTable(TableName.valueOf("ratings_table"));
        //Read data from file
        File file = new File("C:\\Users\\Magdalena\\OneDrive\\Desktop\\archive\\ratings.csv");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String row_key = "";
                String[] data = line.split(",");

                row_key+=data[ID_USER] + "+" + data[RATING] + "+" + data[MOVIE_ID];
                Put p = new Put(Bytes.toBytes(row_key));

                p.addColumn(Bytes.toBytes("ratings"),
                        Bytes.toBytes("id_user"),
                        Bytes.toBytes(data[ID_USER]));
                p.addColumn(Bytes.toBytes("ratings"),
                        Bytes.toBytes("movie_id"),
                        Bytes.toBytes(data[MOVIE_ID]));
                p.addColumn(Bytes.toBytes("ratings"),
                        Bytes.toBytes("rating"),
                        Bytes.toBytes(data[RATING]));
                p.addColumn(Bytes.toBytes("ratings"),
                        Bytes.toBytes("timestamp"),
                        Bytes.toBytes(data[TIMESTAMP]));

                ratingsTable.put(p);
            }
        }
    }
}