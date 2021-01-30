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
        double total = 0;
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table movieTable = connection.getTable(TableName.valueOf("movies_table_3"));
        List<Double> years = new ArrayList<>();
        for(double i = 1870.0; i <= 2020.0; i++) {
            years.add(i);
        }
        for (Double year : years) {
            Scan scan = new Scan();
            scan.setReversed(true);
//            FilterList filterList = new FilterList();
//            filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("movie_info"), Bytes.toBytes("year"), CompareFilter.CompareOp.valueOf("EQUAL"),  new BinaryComparator(Bytes.toBytes(year))));
//            filterList.addFilter(new PageFilter(1));
//            scan.setReversed(true);
//            scan.setFilter(filterList);
            scan.setStartRow(Bytes.toBytes(year+"+X.X"));
            scan.setStopRow(Bytes.toBytes(year.toString()));
            scan.setFilter(new PageFilter(1));
            long startTime = System.nanoTime();
            ResultScanner resultScanner = movieTable.getScanner(scan);
            long elapsedTime = System.nanoTime() - startTime;
            total += elapsedTime;
            for (Result res : resultScanner) {
                //Print movie name
                System.out.print(
                        Bytes.toString(res.getValue(Bytes.toBytes("movie_info"),
                                Bytes.toBytes("title"))) + " ");
                System.out.print(
                        Bytes.toString(res.getValue(Bytes.toBytes("movie_info"),
                                Bytes.toBytes("director"))) + " ");
                System.out.print(
                        Bytes.toString(res.getValue(Bytes.toBytes("movie_info"),
                                Bytes.toBytes("weighted_rating"))) + " ");
                System.out.println(
                        Bytes.toString(res.getValue(Bytes.toBytes("movie_info"),
                                Bytes.toBytes("year"))));

            }
        }
        System.out.println("Total execution time in seconds: "
                + total/1000000000.0);

    }
}

/*

And Then There Were None  7.181870567375888 1870.0
Passage of Venus P.J.C. Janssen 5.5206609535067 1874.0
Sallie Gardner at a Gallop Eadweard Muybridge 5.655261524822695 1878.0
Buffalo Running Eadweard Muybridge 5.269421225937183 1883.0
Man Walking Around a Corner Louis Aimé Augustin Le Prince 4.844532528641572 1887.0
Traffic Crossing Leeds Bridge Louis Aimé Augustin Le Prince 5.525261524822695 1888.0
London's Trafalgar Square William Carr Crofts 5.207174806576402 1890.0
Dickson Greeting William K.L. Dickson  5.110743572695036 1891.0
Poor Pierrot Émile Reynaud 5.557697990543735 1892.0
Blacksmith Scene William K.L. Dickson  5.446586879432624 1893.0
Annie Oakley William K.L. Dickson  5.498126477541372 1894.0
Tables Turned on the Gardener Louis Lumière 6.241970778346351 1895.0
The Arrival of a Train at La Ciotat Auguste Lumière 6.437013864666897 1896.0
The Bewitched Inn Georges Méliès 5.457174806576402 1897.0
The Four Troublesome Heads Georges Méliès 5.816957249802995 1898.0
An Up-to-Date Conjurer Georges Méliès 5.615227478497058 1899.0
The One-Man Band Georges Méliès 5.747643710339679 1900.0
The Man with the Rubber Head Georges Méliès 6.345557679521277 1901.0
A Trip to the Moon Georges Méliès 7.640732640370665 1902.0
The Great Train Robbery Edwin S. Porter 6.680898619134848 1903.0
The Impossible Voyage Georges Méliès 6.155458081930772 1904.0
The Black Imp Georges Méliès 5.777871254884932 1905.0
Dream of a Rarebit Fiend Edwin S. Porter 5.7166467650890125 1906.0
The Dancing Pig  5.567732423681775 1907.0
Fantasmagorie Émile Cohl 5.957391928402567 1908.0
A Corner in Wheat D.W. Griffith 5.675301759410804 1909.0
Frankenstein J. Searle Dawley 5.8277636447733565 1910.0
L'inferno Giuseppe de Liguoro 5.446586879432624 1911.0
The Cameraman's Revenge Wladyslaw Starewicz 6.030099850690558 1912.0
Fantômas: In the Shadow of the Guillotine Louis Feuillade 5.664910239361702 1913.0
Gertie the Dinosaur Winsor McCay 5.898640185312287 1914.0
The Birth of a Nation D.W. Griffith 6.125803413120567 1915.0
Intolerance: Love's Struggle Throughout the Ages D.W. Griffith 6.650160117238385 1916.0
The Immigrant Charlie Chaplin 6.645729905437353 1917.0
A Dog's Life Charlie Chaplin 6.318712240609404 1918.0
Broken Blossoms D.W. Griffith 6.156785414333707 1919.0
The Cabinet of Dr. Caligari Robert Wiene 7.433489463341918 1920.0
The Kid Charlie Chaplin 7.786140527310621 1921.0
Nosferatu F.W. Murnau 7.49489140070922 1922.0
Safety Last! Fred C. Newmeyer 6.94608731071497 1923.0
The Last Laugh F.W. Murnau 6.846078484585322 1924.0
The Gold Rush Charlie Chaplin 7.4560224241672355 1925.0
The General Buster Keaton 7.658602514506768 1926.0
Metropolis Fritz Lang 7.866070886575409 1927.0
The Passion of Joan of Arc Carl Theodor Dreyer 7.681008719017327 1928.0
Man with a Movie Camera Dziga Vertov 7.44708517778095 1929.0
All Quiet on the Western Front Lewis Milestone 7.117491002434635 1930.0
City Lights Charlie Chaplin 7.989803113756496 1931.0
Freaks Tod Browning 7.381808712887308 1932.0
King Kong Merian C. Cooper 7.480356145641369 1933.0
It Happened One Night Frank Capra 7.436841797136357 1934.0
The 39 Steps Alfred Hitchcock 7.108395601148263 1935.0
Modern Times Charlie Chaplin 7.9937944230233215 1936.0
Grand Illusion Jean Renoir 7.022457711114049 1937.0
Bringing Up Baby Howard Hawks 7.259812252073567 1938.0
Gone with the Wind Victor Fleming 7.618753098533361 1939.0
The Great Dictator Charlie Chaplin 7.9770109879764375 1940.0
Citizen Kane Orson Welles 7.926595536739143 1941.0
Casablanca Michael Curtiz 7.8395562401398555 1942.0
Shadow of a Doubt Alfred Hitchcock 7.197305872005361 1943.0
Double Indemnity Billy Wilder 7.795903677150786 1944.0
Children of Paradise Marcel Carné 7.106277995520718 1945.0
It's a Wonderful Life Frank Capra 7.917500607635643 1946.0
Black Narcissus Emeric Pressburger 7.106494265881997 1947.0
Bicycle Thieves Vittorio De Sica 7.786140527310621 1948.0
The Third Man Carol Reed 7.705827664140261 1949.0
Sunset Boulevard Billy Wilder 8.022738893467185 1950.0
Strangers on a Train Alfred Hitchcock 7.40607654949121 1951.0
Singin' in the Rain Stanley Donen 7.784291165587419 1952.0
Tokyo Story Yasujirō Ozu 7.752514184397162 1953.0
Rear Window Alfred Hitchcock 8.135706060976602 1954.0
The Night of the Hunter Charles Laughton 7.655447814836112 1955.0
The Searchers John Ford 7.471977361006434 1956.0
12 Angry Men Sidney Lumet 8.153494545722566 1957.0
Vertigo Alfred Hitchcock 7.921566993725448 1958.0
Some Like It Hot Billy Wilder 7.89208700171191 1959.0
Psycho Alfred Hitchcock 8.257342496512035 1960.0
Yojimbo Akira Kurosawa 7.745570979645968 1961.0
To Kill a Mockingbird Robert Mulligan 7.772736556243827 1962.0
8½ Federico Fellini 7.704146518375242 1963.0
Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb Stanley Kubrick 7.937701188778607 1964.0
For a Few Dollars More Sergio Leone 7.714873598718829 1965.0
Persona Ingmar Bergman 7.7580301327045404 1966.0
The Graduate Mike Nichols 7.509792911785799 1967.0
Once Upon a Time in the West Sergio Leone 8.018590536811182 1968.0
The Wild Bunch Sam Peckinpah 7.4219790414928974 1969.0
They Call Me Trinity Enzo Barboni 7.247091090425532 1970.0
A Clockwork Orange Stanley Kubrick 7.972920591718881 1971.0
The Godfather Francis Ford Coppola 8.481699239394183 1972.0
The Sting George Roy Hill 7.765750284108845 1973.0
The Godfather: Part II Francis Ford Coppola 8.269856846652003 1974.0
One Flew Over the Cuckoo's Nest Miloš Forman 8.26571663092535 1975.0
Taxi Driver Martin Scorsese 8.063522943940518 1976.0
Star Wars George Lucas 8.08572078254651 1977.0
The Deer Hunter Michael Cimino 7.710956739764174 1978.0
Apocalypse Now Francis Ford Coppola 7.9562718637584355 1979.0
The Empire Strikes Back Irvin Kershner 8.18331107102426 1980.0
Das Boot Wolfgang Petersen 7.7624858533272985 1981.0
Blade Runner Ridley Scott 7.876606952298181 1982.0
Scarface Brian De Palma 7.969238431025348 1983.0
Once Upon a Time in America Sergio Leone 8.208617815179423 1984.0
Back to the Future Robert Zemeckis 7.985035972503883 1985.0
Stand by Me Rob Reiner 7.745674168115634 1986.0
Full Metal Jacket Stanley Kubrick 7.865595319957932 1987.0
Grave of the Fireflies Isao Takahata 8.10021376758113 1988.0
Dead Poets Society Peter Weir 8.065514247248975 1989.0
GoodFellas Martin Scorsese 8.168982036811263 1990.0
The Silence of the Lambs Jonathan Demme 8.078777419609372 1991.0
Reservoir Dogs Quentin Tarantino 8.074770666880022 1992.0
Schindler's List Steven Spielberg 8.276720127821376 1993.0
The Shawshank Redemption Frank Darabont 8.486788477479967 1994.0
Dilwale Dulhania Le Jayenge Aditya Chopra 8.91137311995598 1995.0
Trainspotting Danny Boyle 7.76858430428909 1996.0
Life Is Beautiful Roberto Benigni 8.271700840535443 1997.0
American History X Tony Kaye 8.168087382405501 1998.0
Fight Club David Fincher 8.289284020538389 1999.0
Memento Christopher Nolan 8.076853602543267 2000.0
Spirited Away Hayao Miyazaki 8.2739984240543 2001.0
City of God Fernando Meirelles 8.14664318573893 2002.0
The Lord of the Rings: The Return of the King Peter Jackson 8.08822366438559 2003.0
Howl's Moving Castle Hayao Miyazaki 8.151686992077428 2004.0
Pride & Prejudice Joe Wright 7.739566753288939 2005.0
Planet Earth Alastair Fothergill 8.22614071795906 2006.0
There Will Be Blood Paul Thomas Anderson 7.844007234832526 2007.0
The Dark Knight Christopher Nolan 8.291540612117148 2008.0
Inglourious Basterds Quentin Tarantino 7.8863584639664355 2009.0
Inception Christopher Nolan 8.093105293514483 2010.0
The Intouchables Eric Toledano 8.181508850594922 2011.0
Paperman John Kahrs 7.877913773068091 2012.0
The Wolf of Wall Street Martin Scorsese 7.886699351975506 2013.0
Whiplash Damien Chazelle 8.276403466671812 2014.0
Room Lenny Abrahamson 8.066138423769358 2015.0
Your Name. Makoto Shinkai 8.395883278393766 2016.0
In a Heartbeat Beth David 7.72494857176443 2017.0
Iron Sky: The Coming Race Timo Vuorensola 5.089019756838907 2018.0
Avatar 2 James Cameron 1.9152224891329213 2020.0

Total execution time in seconds: 0.0109988

 */

