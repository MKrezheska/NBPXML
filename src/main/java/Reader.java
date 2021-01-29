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
public class Reader
{
    public static void main( String[] args ) throws IOException
    {
        int ID_GENRE = 0;
        int NAME_GENRE = 1;
        int MOVIE_ID = 2;
        int BUDGET= 3;
        int POPULARITY = 4;
        int REVENUE = 5;
        int RUNTIME = 6;
        int TITLE = 7;
        int VOTE_AVERAGE = 8;
        int VOTE_COUNT = 9;
        int WEIGHTED_RATING = 10;
        int DIRECTOR = 11;
        int YEAR = 12;
        //HBase configuration
        Configuration config = HBaseConfiguration.create();
        //Get connection instance from configuration
        Connection connection = ConnectionFactory.createConnection(config);
        //Get movie table instance
        Table movieTable = connection.getTable(TableName.valueOf("movies_table"));
        //Read data from file
        File file = new File("C:\\Users\\Magdalena\\OneDrive\\Desktop\\archive\\genres_movies_years.csv");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String row_key = "";
                String[] data = line.split(",");

                row_key+=data[ID_GENRE] + "+" + data[MOVIE_ID];
                Put p = new Put(Bytes.toBytes(row_key));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("id_genre"),
                        Bytes.toBytes(data[ID_GENRE]));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("name_genre"),
                        Bytes.toBytes(data[NAME_GENRE]));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("movie_id"),
                        Bytes.toBytes(data[MOVIE_ID]));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("budget"),
                        Bytes.toBytes(data[BUDGET]));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("popularity"),
                        Bytes.toBytes(data[POPULARITY]));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("revenue"),
                        Bytes.toBytes(data[REVENUE]));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("runtime"),
                        Bytes.toBytes(data[RUNTIME]));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("title"),
                        Bytes.toBytes(data[TITLE]));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("vote_average"),
                        Bytes.toBytes(data[VOTE_AVERAGE]));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("vote_count"),
                        Bytes.toBytes(data[VOTE_COUNT]));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("weighted_rating"),
                        Bytes.toBytes(data[WEIGHTED_RATING]));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("director"),
                        Bytes.toBytes(data[DIRECTOR]));
                p.addColumn(Bytes.toBytes("cf"),
                        Bytes.toBytes("year"),
                        Bytes.toBytes(data[YEAR]));
                movieTable.put(p);

            }
        }
    }
}