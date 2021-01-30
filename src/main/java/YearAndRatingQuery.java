import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class YearAndRatingQuery {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table movieTable = connection.getTable(TableName.valueOf("movies_table_3"));

        Scan scan = new Scan();
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("movie_info"), Bytes.toBytes("weighted_rating"), CompareFilter.CompareOp.valueOf("GREATER"), new BinaryComparator(Bytes.toBytes(Double.toString(4)))));
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("movie_info"), Bytes.toBytes("year"), CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(Double.toString(1998.0)))));
        scan.setFilter(filterList);
        long startTime = System.nanoTime();
        ResultScanner resultScanner = movieTable.getScanner(scan);
        long elapsedTime = System.nanoTime() - startTime;
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

        System.out.println("Total execution time in seconds: "
                + elapsedTime / 1000000000.0);

    }
}

/*

The Second Arrival Kevin Tenney 4.311928191489361 1998.0
Addams Family Reunion Dave Payne 4.461922117152613 1998.0
Chairman of the Board Alex Zamm 4.498243572695035 1998.0
The Avengers Jeremiah S. Chechik 4.518815381205674 1998.0
3 Ninjas: High Noon at Mega Mountain Sean McNamara 4.52350262314194 1998.0
Children of the Corn V: Fields of Terror Ethan Wiley 4.530084947602413 1998.0
Curse of the Puppet Master David DeCoteau 4.613012572533849 1998.0
The Patriot Dean Semler 4.6254902589079725 1998.0
Species II Peter Medak 4.628141283108988 1998.0
Tale of the Mummy Russell Mulcahy 4.658170026129152 1998.0
Sometimes They Come Back... for More Daniel Zelik Berk 4.714313829787233 1998.0
Monella Tinto Brass 4.7144894998618385 1998.0
Nick Fury: Agent of S.H.I.E.L.D. Rod Hardy 4.730121095518334 1998.0
Barney's Great Adventure Steve Gomer 4.749202127659574 1998.0
Creature Stuart Gillard 4.752065919452886 1998.0
Tarzan and the Lost City Carl Schenkel 4.764793144208038 1998.0
Krippendorf's Tribe Todd Holland 4.8065982776089164 1998.0
Meet the Deedles Steve Boyum 4.8169572498029956 1998.0
Ringmaster Neil Abramson 4.819076906028369 1998.0
Carnival of Souls Adam Grossman 4.8208111702127665 1998.0
Psycho Gus Van Sant 4.855655565258812 1998.0
Ernest In The Army John R. Cherry III 4.869421225937184 1998.0
The Dentist 2: Brace Yourself Brian Yuzna 4.880285299806577 1998.0
Major League: Back to the Minors John Warren 4.880285299806577 1998.0
Knock Off Tsui Hark 4.883156914893617 1998.0
Air Bud: Golden Receiver Richard Martin 4.88573278275476 1998.0
Bimboland Ariel Zeitoun 4.902182858289843 1998.0
Break Up Paul Marcus 4.911720261121856 1998.0
Universal Soldier II: Brothers in Arms Jeff Woolnough 4.916643988124691 1998.0
Pocahontas II: Journey to a New World Bradley Raymond 4.934685817739042 1998.0
Telling You Robert DeFranco 4.935559229442209 1998.0
Since You've Been Gone David Schwimmer 4.935904255319148 1998.0
Blackjack John Woo 4.9383138297872335 1998.0
Scorpio One Worth Keeter 4.9516843971631195 1998.0
Legion Jon Hess 4.9516843971631195 1998.0
T.N.T. Robert Radler 4.9516843971631195 1998.0
Strangeland John Pieplow 4.957967485765657 1998.0
This Is Not an Exit: The Fictional World of Bret Easton Ellis Gerald Fox 4.9598835537665344 1998.0
Buttcrack Jim Larsen 4.962892287234042 1998.0
The Christmas Wish Ian Barry 4.964504909983632 1998.0
The Revengers' Comedies Malcolm Mowbray 4.964659321175279 1998.0
Futuresport Ernest R. Dickerson 4.9738809714973895 1998.0
Holy Man Stephen Herek 4.977808726279472 1998.0
Diamond Girl Timothy Bond 4.990145935624659 1998.0
McCinsey's Island Sam Firstenberg 4.990145935624659 1998.0
Better Living Max Mayer 5.003213652482269 1998.0
The Secret of NIMH 2: Timmy to the Rescue Dick Sebast 5.009237588652481 1998.0
The Quarry Marion Hänsel 5.0166403105232895 1998.0
Men in White Scott P. Levy 5.0166403105232895 1998.0
Lost in Space Stephen Hopkins 5.018873701138051 1998.0
Home Fries Dean Parisot 5.02262226103525 1998.0
The Peacekeeper Frédéric Forestier 5.0253566247582215 1998.0
The Alarmist Evan Dunsky 5.03462346024636 1998.0
Airspeed Robert Tinnell 5.03462346024636 1998.0
Phantasm IV: Oblivion Don Coscarelli 5.038743270956165 1998.0
Firestorm Dean Semler 5.052224836333878 1998.0
Bone Daddy Mario Philip Azzopardi 5.05718085106383 1998.0
A Letter from Death Row Bret Michaels 5.058769208037824 1998.0
Logan's War:  Bound by Honor Michael Preece 5.063155616031667 1998.0
20 Dates Myles Berkowitz 5.076728723404255 1998.0
Relax... It's Just Sex P.J. Castellaneta 5.076728723404255 1998.0
The Ransom of Red Chief  5.079889525368248 1998.0
Beefcake Thom Fitzgerald 5.082570921985815 1998.0
A Kid in Aladdin's Palace Robert L. Levy 5.082892287234042 1998.0
La Cucaracha Jack Perez 5.087255039193728 1998.0
The Wonderful Ice Cream Suit Stuart Gordon 5.089019756838907 1998.0
Trans Julian Goldberger 5.089019756838907 1998.0
Bo Ba Bu Ali Khamrayev 5.089019756838907 1998.0
Nothing Too Good for a Cowboy Kari Skogland 5.089019756838907 1998.0
Kestrel's Eye Mikael Kristersson 5.089019756838907 1998.0
With Friends Like These... Philip Frank Messina 5.089019756838907 1998.0
Dibu 2: La venganza de Nasty Carlos Galettini 5.089019756838907 1998.0
Home Page  5.089019756838907 1998.0
Ingmar Bergman on Life and Work Jörn Donner 5.089019756838907 1998.0
Mumia Abu-Jamal: A Case for Reasonable Doubt? John Edginton 5.089019756838907 1998.0
Somewhere in the City Ramin Niami 5.089019756838907 1998.0
Louise Brooks: Looking for Lulu Hugh Munro Neely 5.089019756838907 1998.0
Such a Long Journey Sturla Gunnarsson 5.089019756838907 1998.0
Fallout Rodney McDonald 5.089019756838907 1998.0
A Tatsuya Mori 5.089019756838907 1998.0
The Footprints of a Spirit Carlos Rodríguez 5.089019756838907 1998.0
Detour Joey Travolta 5.089019756838907 1998.0
Went to Coney Island on a Mission from God... Be Back by Five  5.089019756838907 1998.0
VeggieTales: The End of Silliness? More Really Silly Songs! Mike Nawrocki 5.089019756838907 1998.0
Amarillo By Morning Spike Jonze 5.089019756838907 1998.0
Type O Negative: After Dark Thomas Mignone 5.089019756838907 1998.0
Men with Guns Kari Skogland 5.089019756838907 1998.0
Top of the World Sidney J. Furie 5.089019756838907 1998.0
Very Private Lesson Hideaki Oba 5.089019756838907 1998.0
Gringuito  5.089019756838907 1998.0
Photographer Dariusz Jabłoński 5.089019756838907 1998.0
Jeff Foxworthy: Totally Committed Keith Truesdell 5.089019756838907 1998.0
Midnight Walter Salles 5.089019756838907 1998.0
Sam Kinison: Why Did We Laugh? Larry Carroll 5.089019756838907 1998.0
David Spade: Take the Hit Keith Truesdell 5.089019756838907 1998.0
Advertising and the End of the World  5.089019756838907 1998.0
Restaurant Eric Bross 5.089019756838907 1998.0
Titanic Town Roger Michell 5.089019756838907 1998.0
A Paralyzing Fear: The Story of Polio in America  5.089019756838907 1998.0
Captive Matt Dorff 5.089019756838907 1998.0
Frogs for Snakes Amos Poe 5.089019756838907 1998.0
A Change of Heart Arvin Brown 5.089019756838907 1998.0
Girl Jonathan Kahn 5.08969556360344 1998.0
Rear Window Jeff Bleckner 5.095181211684096 1998.0
The Eternal Michael Almereyda 5.097992654508611 1998.0
Marie from the Bay of Angels Manuel Pradal 5.1083076707726764 1998.0
I Still Know What You Did Last Summer Danny Cannon 5.111335796849426 1998.0
Fire-Eater Pirjo Honkasalo 5.1143247635933795 1998.0
Provocateur Jim Donovan 5.1143247635933795 1998.0
Babymother Julian Henriques 5.1143247635933795 1998.0
The Hi-Lo Country Stephen Frears 5.116265715667312 1998.0
FernGully 2: The Magical Rescue Dave Marshall 5.117355138071527 1998.0
Caught Up Darin Scott 5.118351063829786 1998.0
I Think I Do Brian Sloan 5.125946313706086 1998.0
CHiPs '99  5.1360878926038485 1998.0
New Rose Hotel Abel Ferrara 5.137554093040028 1998.0
Gallo cedrone Carlo Verdone 5.137554093040028 1998.0
The Lighthouse Eduardo Mignogna 5.138261932144912 1998.0
Hi-Life Roger Hedden 5.138261932144912 1998.0
Figli di Annibale Davide Ferrario 5.138261932144912 1998.0
Urban Ghost Story Geneviève Jolliffe 5.138261932144912 1998.0
Painted Lady Julian Jarrold 5.138261932144912 1998.0
The Adopted Son Aktan Arym Kubat 5.138261932144912 1998.0
Letters From a Killer David Carson 5.1423805524449415 1998.0
Purgatory Aleksandr Nevzorov 5.143992089470811 1998.0
Pápa Piquillo Álvaro Sáenz de Heredia 5.1468765462642265 1998.0
Madeline Daisy von Scherler Mayer 5.1582184134489095 1998.0
Jeanne and the Perfect Guy Olivier Ducastel 5.160939249720045 1998.0
Those Who Love Me Can Take the Train Patrice Chéreau 5.160939249720045 1998.0
T-Rex: Back to the Cretaceous Brett Leonard 5.160939249720045 1998.0
A Life Apart: Hasidism in America Menachem Daum 5.162586256469235 1998.0
The Minion Jean-Marc Piché 5.163797353399065 1998.0
The New Swiss Family Robinson Stewart Raffill 5.163797353399065 1998.0
Modern Vampires Richard Elfman 5.169076906028369 1998.0
My Giant Michael Lehmann 5.169348404255318 1998.0
Pablo Escobar: King of Cocaine  5.169880319148937 1998.0
The Three Men of Melita Zganjer Snježana Tribuson 5.169880319148937 1998.0
Legalese Glenn A. Jordan 5.169880319148937 1998.0
Standoff Andrew Chapman 5.172892287234042 1998.0
I've Been Waiting for You Christopher Leitch 5.174183130699088 1998.0
The Last Broadcast Stefan Avalos 5.1748522458628825 1998.0
The Very Thought of You Nick Hamm 5.176428075855688 1998.0
A Knight in Camelot Roger Young 5.178594858156028 1998.0
Among Giants Sam Miller 5.178802472685452 1998.0
I Woke Up Early The Day I Died Aris Iliopulos 5.181991881298992 1998.0
Southie John Shea 5.181991881298992 1998.0
Freedom Strike Jerry P. Jacobs 5.1824536279323485 1998.0
Montana Jennifer Leitzes 5.1824536279323485 1998.0
Webmaster Thomas Borch Nielsen 5.184447533849129 1998.0
Dead Man's Curve Dan Rosen 5.1857398452611205 1998.0
The Substitute 2: School's Out Steven Pearl 5.187327620173366 1998.0
Devil in the Flesh Steve Cohen 5.188737011380503 1998.0
Bade Miyan Chote Miyan David Dhawan 5.193230749746707 1998.0
Dallas - War of The Ewings  5.195274140752863 1998.0
Mars Jon Hess 5.195274140752863 1998.0
Beck 06 - Monstret Harald Hamrell 5.202892287234042 1998.0
Listening to You: The Who Live at the Isle of Wight Murray Lerner 5.2031267970097765 1998.0
Vig Graham Theakston 5.2031267970097765 1998.0
Blues Brothers 2000 John Landis 5.206545728802794 1998.0
Fifteen and Pregnant Sam Pillsbury 5.207174806576402 1998.0
Some Girl Rory Kelly 5.208094653573376 1998.0
Phantoms Joe Chappelle 5.209073734409392 1998.0
Legionnaire Peter MacDonald 5.209331145314622 1998.0
Twice Upon a Yesterday María Ripoll 5.209667243938643 1998.0
I'll Be Home for Christmas Arlene Sanford 5.213156914893617 1998.0
The Con Steven Schachter 5.213570828667413 1998.0
Tommy and the Wildcat Raimo O. Niemi 5.213570828667413 1998.0
Slayers Gorgeous Hiroshi Watanabe 5.213570828667413 1998.0
Day of the Full Moon Karen Shakhnazarov 5.213570828667413 1998.0
Black Dog Kevin Hooks 5.216446143617022 1998.0
Gunshy Jeff Celentano 5.219343013225992 1998.0
Going to Kansas City Pekka Mandart 5.219343013225992 1998.0
The Fairy King Of Ar Paul Matthews 5.219343013225992 1998.0
Money Play$ Frank D. Gilroy 5.219343013225992 1998.0
Forever Fever Glen Goei 5.220915166393889 1998.0
An All Dogs Christmas Carol Paul Sabella 5.22392166344294 1998.0
Praise John Curran 5.225435874704491 1998.0
About Sarah Susan Rohrer 5.225435874704491 1998.0
Animals with the Tollkeeper Michael Di Jiacomo 5.225435874704491 1998.0
The Shoe Laila Pakalnina 5.225435874704491 1998.0
The Unknown Cyclist Bernard Salzmann 5.225435874704491 1998.0
30 Years to Life Michael Tuchner 5.225435874704491 1998.0
Evasive Action Jerry P. Jacobs 5.225435874704491 1998.0
In God's Hands Zalman King 5.225435874704491 1998.0
Johnny Skidmarks John Raffo 5.225435874704491 1998.0
A Brooklyn State of Mind  5.225435874704491 1998.0
Phoenix Danny Cannon 5.2268508467216686 1998.0
Wide Awake M. Night Shyamalan 5.2279934359438665 1998.0
Billy's Hollywood Screen Kiss Tommy O'Haver 5.228601988899167 1998.0
Перекресток Dmitriy Astrakhan 5.243667337550317 1998.0
Dollar for the Dead Gene Quintano 5.243667337550317 1998.0
Alaska: Spirit of the Wild George Casey 5.245149776035834 1998.0
The Love Letter Dan Curtis 5.246556192034915 1998.0
Judas Kiss Sebastian Gutierrez 5.252629352030947 1998.0
Wicked Michael Steinberg 5.259131205673759 1998.0
China Gate Rajkumar Santoshi 5.259376704855428 1998.0
Where's Marlowe? Daniel Pyne 5.2598835537665325 1998.0
Twilight of the Dark Master Akiyuki Shinbo 5.2598835537665325 1998.0
Tainted Brian Evans 5.2598835537665325 1998.0
Hard John Huckert 5.2598835537665325 1998.0
Alice Through the Looking Glass John Henderson 5.261401694247438 1998.0
Rasen George Iida 5.2646727371078255 1998.0
For Sale Laetitia Masson 5.266202407614781 1998.0
The Red Dwarf Yvan Le Moine 5.266202407614781 1998.0
Ruby Bridges Euzhan Palcy 5.266202407614781 1998.0
Basil Radha Bharadwaj 5.269421225937183 1998.0
The Tale of Sweeney Todd John Schlesinger 5.2721972176759415 1998.0
Siberia Robert Jan Westdijk 5.2721972176759415 1998.0
I Got the Hook Up Michael Martin 5.2724579416130615 1998.0
Jeremiah Harry Winer 5.2753566247582215 1998.0
Postmortem Albert Pyun 5.277892287234042 1998.0
Paparazzi Alain Berbérian 5.280775901942644 1998.0
My Own Country Mira Nair 5.280991430260047 1998.0
The Blood Oranges Philip Haas 5.280991430260047 1998.0
The Day Lincoln Was Shot John Gray 5.280991430260047 1998.0
Black Sun: The Mythological Background of National Socialism Rüdiger Sünner 5.280991430260047 1998.0
Veranda för en tenor Lisa Ohlin 5.280991430260047 1998.0
Inside The X-Files Glen Kasper 5.280991430260047 1998.0
Medusa George Lazopoulos 5.280991430260047 1998.0
Legacy T. J. Scott 5.280991430260047 1998.0
Don't Tell Anyone Francisco J. Lombardi 5.283309548521017 1998.0
Hell's Kitchen Tony Cinciripini 5.283312584880036 1998.0
Living Out Loud Richard LaGravenese 5.283312584880036 1998.0
Summer of the Monkeys Michael Anderson 5.287255039193728 1998.0
Final Cut Dominic Anciano 5.288468844984802 1998.0
Curtain Call Peter Yates 5.292892287234042 1998.0
Babe: Pig in the City George Miller 5.29399334723159 1998.0
Polish Wedding Theresa Connelly 5.296522480931354 1998.0
Godzilla Roland Emmerich 5.29812224458501 1998.0
Up 'n' Under John Godber 5.300382719252723 1998.0
Broken Vessels Scott Ziehl 5.300424094307074 1998.0
Outside Ozona J.S. Cardone 5.300424094307074 1998.0
Brown's Requiem Jason Freeland 5.300424094307074 1998.0
The First Snow of Winter Graham Ralph 5.300424094307074 1998.0
Woo Daisy von Scherler Mayer 5.302570921985815 1998.0
Like It Is Paul Oremland 5.306862858464385 1998.0
Fiona Amos Kollek 5.307892287234043 1998.0
In The Winter Dark James Bogle 5.308769208037824 1998.0
Dog Park Bruce McCulloch 5.310658756137478 1998.0
Poliisin poika Tapio Piirainen 5.310658756137478 1998.0
No Looking Back Edward Burns 5.310658756137478 1998.0
Billboard Dad Alan Metter 5.312947483958122 1998.0
The Pear Tree Dariush Mehrjui 5.31883398656215 1998.0
Claire Dolan Lodge Kerrigan 5.31883398656215 1998.0
The Last Big Thing Dan Zukovic 5.31883398656215 1998.0
The Naked Man J. Todd Anderson 5.31883398656215 1998.0
The Proposition Lesli Linka Glatter 5.322892287234042 1998.0
Lulu on the Bridge Paul Auster 5.322892287234042 1998.0
The Conman Wong Jing 5.322892287234042 1998.0
Don Juan Jacques Weber 5.3234792689579935 1998.0
Homegrown Stephen Gyllenhaal 5.3257978723404245 1998.0
The School of Flesh Benoît Jacquot 5.32656408308004 1998.0
Out of Mind: The Stories of H.P. Lovecraft Raymond Saint-Jean 5.336546985815601 1998.0
Shadow Run  5.336546985815601 1998.0
The Dancemaker  5.336546985815601 1998.0
Monk Dawson Tom Waller 5.336546985815601 1998.0
A Soldier's Sweetheart Thomas Michael Donnelly 5.336546985815601 1998.0
Glorious Technicolor Peter Jones 5.336546985815601 1998.0
Nô Robert Lepage 5.336546985815601 1998.0
La Patinoire Jean-Philippe Toussaint 5.336546985815601 1998.0
Men Cry Bullets Tamara Hernandez 5.336546985815601 1998.0
Un grand cri d'amour Josiane Balasko 5.336546985815601 1998.0
Hardly a Butterfly Heidi Köngäs 5.336546985815601 1998.0
Storefront Hitchcock Jonathan Demme 5.336546985815601 1998.0
Off the Menu: The Last Days of Chasen's Shari Springer Berman 5.336546985815601 1998.0
The Sleep Room  5.336546985815601 1998.0
Jane Austen's Mafia! Jim Abrahams 5.336961971142089 1998.0
Soldier Mastan Alibhai Burmawalla 5.337892287234042 1998.0
Let the Women Wait! Stavros Tsiolis 5.339886618141096 1998.0
Blind Faith Ernest R. Dickerson 5.340964634847613 1998.0
The Inspectors Brad Turner 5.340964634847613 1998.0
Demons of War Władysław Pasikowski 5.340964634847613 1998.0
The Adventures of Sebastian Cole Tod Williams 5.340964634847613 1998.0
Saint Maybe Michael Pressman 5.340964634847613 1998.0
A Hole in the Head Eli Kabillio 5.340964634847613 1998.0
Free Money Yves Simoneau 5.343538442940039 1998.0
A Price Above Rubies Boaz Yakin 5.351602231447847 1998.0
Casper Meets Wendy Sean McNamara 5.353522809231879 1998.0
Duplicate Mahesh Bhatt 5.359131205673759 1998.0
Slappy and the Stinkers Barnet Kellman 5.359131205673759 1998.0
Can't You Hear the Wind Howl? The Life & Music of Robert Johnson Peter Meyer 5.3643247635933795 1998.0
Evidence of Blood Andrew Mondshein 5.364659321175278 1998.0
Safe House Eric Steven Stahl 5.364659321175278 1998.0
U Pana Boga za piecem Jacek Bromski 5.365288959171938 1998.0
Subspecies 4: Bloodstorm Ted Nicolaou 5.366265715667312 1998.0
Rebirth of Mothra III Okihiro Yoneda 5.367892287234042 1998.0
Young Thugs: Nostalgia Takashi Miike 5.367892287234042 1998.0
Ground Control Richard Howard 5.368220594138898 1998.0
Alice and Martin André Téchiné 5.368675402179553 1998.0
Free Enterprise Robert Meyer Burnett 5.375904255319148 1998.0
August 32nd on Earth Denis Villeneuve 5.377109104403759 1998.0
Music from Another Room Charlie Peters 5.379912071211463 1998.0
The Tempest Jack Bender 5.3815051753881535 1998.0
Sada Nobuhiko Ôbayashi 5.3815051753881535 1998.0
Resurrection Man Marc Evans 5.3815051753881535 1998.0
Tomorrow Night Louis C.K. 5.3815051753881535 1998.0
The Chosen One: Legend of the Raven Lawrence Lanoff 5.3815051753881535 1998.0
The Class Trip Claude Miller 5.383706940222896 1998.0
Hurlyburly Anthony Drazan 5.384485815602837 1998.0
Goodbye Lover Roland Joffé 5.385123728029601 1998.0
Poodle Springs Bob Rafelson 5.3875818330605565 1998.0
Sour Grapes Larry David 5.388992988394583 1998.0
Dance with Me Randa Haines 5.391791381175622 1998.0
Déjà Vu Henry Jaglom 5.392102541371158 1998.0
Six Days in Roswell Timothy Johnson 5.392102541371158 1998.0
Doctor Dolittle Betty Thomas 5.392588772282922 1998.0
The Brave Little Toaster Goes to Mars Robert C. Ramirez 5.393950882752375 1998.0
Place Vendome Nicole Garcia 5.3978922872340425 1998.0
The Silence Mohsen Makhmalbaf 5.3978922872340425 1998.0
Cousin Bette Des McAnuff 5.400348699763591 1998.0
The Outskirts Pyotr Lutsik 5.400402345881068 1998.0
Jinnah Jamil Dehlavi 5.400402345881068 1998.0
A Bright Shining Lie Terry George 5.402754559270517 1998.0
The Boys Rowan Woods 5.403044512877939 1998.0
Almost Heroes Christopher Guest 5.4034314292321906 1998.0
History of Cinema in Popielawy Jan Jakub Kolski 5.405829499712478 1998.0
B. Monkey Michael Radford 5.410524316109423 1998.0
Fögi Is a Bastard Marcel Gisler 5.411210684551341 1998.0
I Want You Michael Winterbottom 5.411210684551341 1998.0
Bedrooms and Hallways Rose Troche 5.411210684551341 1998.0
Down in the Delta Maya Angelou 5.41289228723404 1998.0
World War Three Robert Stone 5.413222858701581 1998.0
Shadrach Susanna Styron 5.413222858701581 1998.0
Beck 04 - Öga för öga Kjell Sundvall 5.419894914374675 1998.0
The Polar Bear Til Schweiger 5.421610457220462 1998.0
Anna Magdalena Yee Chung-Man 5.422045715928695 1998.0
Johnny 2.0 Neill Fearnley 5.422045715928695 1998.0
The Personals Chen Kuo-Fu 5.422045715928695 1998.0
The Land Girls David Leland 5.424097144456887 1998.0
A True Mob Story Wong Jing 5.424097144456887 1998.0
Megacities Michael Glawogger 5.424097144456887 1998.0
Dancing at Lughnasa Pat O'Connor 5.424793144208038 1998.0
Le Poulpe Guillaume Nicloux 5.427892287234042 1998.0
Disturbing Behavior David Nutter 5.436084111434257 1998.0
Dad Savage Betsan Morris Evans 5.436968085106383 1998.0
L.A. Without a Map Mika Kaurismäki 5.43729764107308 1998.0
The Polish Bride Karim Traïdia 5.438863884342606 1998.0
Body Count Robert Patton-Spruill 5.438863884342606 1998.0
Talk of Angels Nick Hamm 5.438863884342606 1998.0
CounterForce Martin Kunert 5.438863884342606 1998.0
Ride Millicent Shelton 5.438863884342606 1998.0
Dulhe Raja Harmesh Malhotra 5.4398998020781795 1998.0
Head On Ana Kokkinos 5.439918707346448 1998.0
Illuminata John Turturro 5.440849797365755 1998.0
Das merkwürdige Verhalten geschlechtsreifer Großstädter zur Paarungszeit Marc Rothemund 5.440849797365755 1998.0
Of Freaks and Men Aleksey Balabanov 5.441136561007379 1998.0
Ayn Rand: A Sense of Life Michael Paxton 5.4451497760358345 1998.0
Fearful Symmetry Charles Kiselyak 5.447658096926713 1998.0
54 Mark Christopher 5.448645615545443 1998.0
Short Sharp Shock Fatih Akin 5.4492375886524815 1998.0
Dead Man on Campus Alan Cohn 5.45375823201621 1998.0
Palmetto Volker Schlöndorff 5.454932679521277 1998.0
Edge of Seventeen David Moreton 5.454932679521277 1998.0
The Brandon Teena Story Susan Muska 5.455676091825309 1998.0
Little Tony Alex van Warmerdam 5.455676091825309 1998.0
Divorcing Jack David Caffrey 5.457892287234042 1998.0
Hard Rain Mikael Salomon 5.458868942080379 1998.0
Kissing a Fool Doug Ellin 5.459131205673758 1998.0
Welcome to Woop Woop Stephan Elliott 5.459897416413374 1998.0
Monument Ave. Ted Demme 5.460830034636318 1998.0
Spoiler Jeff Burr 5.462586256469235 1998.0
Your Friends & Neighbors Neil LaBute 5.467609451718495 1998.0
Jack Frost Troy Miller 5.4692584480600726 1998.0
David and Lisa Lloyd Kramer 5.472892287234043 1998.0
Lenny Bruce: Swear to Tell the Truth Robert B. Weide 5.475435874704491 1998.0
On Our Own Lone Scherfig 5.475435874704491 1998.0
Overnight Delivery Jason Bloom 5.4765371438874855 1998.0
Reach The Rock William Ryan 5.476728723404255 1998.0
L'ennui Cédric Kahn 5.476929606156632 1998.0
The Dinner Ettore Scola 5.477325422804147 1998.0
Bride of Chucky Ronny Yu 5.477844617180454 1998.0
An American Tail: The Treasure of Manhattan Island Larry Latham 5.479094246329037 1998.0
The Longest Nite Patrick Yau 5.481760267194458 1998.0
The Gingerbread Man Robert Altman 5.48216238179669 1998.0
Bio Zombie Wilson Yip 5.482313829787233 1998.0
Whatever Susan Skoog 5.48691058079356 1998.0
Whispering Corridors Ki-hyeong Park 5.4871424834321605 1998.0
A Place Called Chiapas Nettie Wild 5.4878922872340405 1998.0
The Object of My Affection Nicholas Hytner 5.488324468085105 1998.0
Hamilton Harald Zwart 5.4924645390070905 1998.0
Goodnight Mister Tom Jack Gold 5.4924645390070905 1998.0
The Wisdom of Crocodiles Po-Chih Leong 5.497074468085106 1998.0
Am I Beautiful? Doris Dörrie 5.497781354983204 1998.0
The Inheritors Stefan Ruzowitzky 5.497992654508612 1998.0
Pyaar Kiya To Darna Kya Sohail Khan 5.498126477541372 1998.0
Young and Dangerous 5 Andrew Lau 5.498126477541372 1998.0
Spriggan Hirotsugu Kawasaki 5.50222483633388 1998.0
L'odore della notte Claudio Caligari 5.502629352030947 1998.0
A Soldier's Daughter Never Cries James Ivory 5.502892287234042 1998.0
Cabaret Balkan Goran Paskaljević 5.502966448445172 1998.0
Modulations Iara Lee 5.503126797009777 1998.0
The Misadventures of Margaret Brian Skeet 5.503213652482269 1998.0
The Danube Exodus Forgács Péter 5.503213652482269 1998.0
Stiff Upper Lips Gary Sinyor 5.503213652482269 1998.0
Tango Carlos Saura 5.505260768033213 1998.0
The Impostors Stanley Tucci 5.511616820553996 1998.0
Milk Andrea Arnold 5.515786961265683 1998.0
Slam Marc Levin 5.517040273556232 1998.0
The Land Before Time VI: The Secret of Saurus Rock Charles Grosvenor 5.52176922147002 1998.0
Party Monster: The Shockumentary Fenton Bailey 5.522570921985817 1998.0
The Decline of Western Civilization Part III Penelope Spheeris 5.5236207323107385 1998.0
The Rose Seller Víctor Gaviria 5.525356624758222 1998.0
Rudolph the Red-Nosed Reindeer: The Movie William R. Kowalchuk Jr. 5.527367021276596 1998.0
It's a Long Road Pantelis Voulgaris 5.5293603023516225 1998.0
Six-String Samurai Lance Mungia 5.531206922385375 1998.0
Beloved Jonathan Demme 5.531404778514126 1998.0
My Best Friend's Wife Vincenzo Salemme 5.532248755092802 1998.0
Always Outnumbered Michael Apted 5.5328922872340405 1998.0
Orphans Peter Mullan 5.536087892603849 1998.0
Ghulam Vikram Bhatt 5.536840220949261 1998.0
Next Stop Wonderland Brad Anderson 5.537780205167173 1998.0
Barking at the Stars Zdravko Šotra 5.539407109496627 1998.0
Leather Jacket Love Story David DeCoteau 5.543667337550316 1998.0
Bloody Angels Karin Julsrud 5.543667337550316 1998.0
The Players Club Ice Cube 5.546313829787233 1998.0
42 Up Michael Apted 5.548083897485493 1998.0
Earth Deepa Mehta 5.548083897485493 1998.0
Alien Abduction: Incident in Lake County Dean Alioto 5.548682679521277 1998.0
Kurt & Courtney Nick Broomfield 5.5540875613747955 1998.0
Why Do Fools Fall In Love Gregory Nava 5.559908329560886 1998.0
How Stella Got Her Groove Back Kevin Rodney Sullivan 5.561282906599351 1998.0
Urban Legend Jamie Blanks 5.562547839906591 1998.0
Secret Defense Jacques Rivette 5.5628922872340425 1998.0
The Governess Sandra Goldbacher 5.565481197427014 1998.0
Divorce Iranian Style Kim Longinotto 5.567069012547735 1998.0
Only Clouds Move the Stars Torun Lian 5.567069012547735 1998.0
Six Days Seven Nights Ivan Reitman 5.567501829336934 1998.0
Shark Skin Man and Peach Hip Girl Katsuhito Ishii 5.570811170212767 1998.0
Country of the Deaf Valery Todorovsky 5.570811170212767 1998.0
Jerry and Tom Saul Rubinek 5.570811170212767 1998.0
The Swan Princess: The Mystery of the Enchanted Kingdom Richard Rich 5.572485296661478 1998.0
Dr. Akagi Shôhei Imamura 5.573553450960041 1998.0
Wrongfully Accused Pat Proft 5.575521361026682 1998.0
Swept from the Sea Beeban Kidron 5.5875679040289725 1998.0
The Terrorist Santosh Sivan 5.605530551009274 1998.0
Safe Men John Hamburg 5.606070990180035 1998.0
Birdcage Inn Kim Ki-duk 5.606070990180035 1998.0
Brave New World Larry Williams 5.606576906028369 1998.0
Permanent Midnight David Veloz 5.613667037154652 1998.0
Hush Jonathan Darby 5.615388499865339 1998.0
The Newton Boys Richard Linklater 5.615388499865339 1998.0
The Book of Life Hal Hartley 5.616265715667311 1998.0
The Grandfather José Luis Garci 5.619906336725254 1998.0
Senseless Penelope Spheeris 5.628069292696952 1998.0
Xiu Xiu: The Sent-Down Girl Joan Chen 5.6313259878419455 1998.0
The Prophecy II Greg Spence 5.63232966058764 1998.0
The Odd Couple II Howard Deutch 5.635169706180344 1998.0
Hideous Kinky Gillies MacKinnon 5.6364774394717525 1998.0
Brink! Greg Beeman 5.6364774394717525 1998.0
Hope Floats Forest Whitaker 5.6368862520458265 1998.0
Babylon 5: The River of Souls Janet Greek 5.642432679521277 1998.0
Everest David Breashears 5.6439216634429386 1998.0
Left Luggage Jeroen Krabbé 5.6447931442080375 1998.0
The Opposite of Sex Don Roos 5.652919686696283 1998.0
Halloween: H20 Steve Miner 5.6540049603793605 1998.0
When Trumpets Fade John Irvin 5.655261524822695 1998.0
Desperate Measures Barbet Schroeder 5.663323068309071 1998.0
Love & Pop Hideaki Anno 5.669237588652482 1998.0
The Pentagon Wars Richard Benjamin 5.670546627433227 1998.0
One True Thing Carl Franklin 5.672833733699383 1998.0
Without Limits Robert Towne 5.6821034816247575 1998.0
Merlin Steve Barron 5.684337701089777 1998.0
Twilight Robert Benton 5.68754495055439 1998.0
The Rugrats Movie Norton Virgien 5.6962809320074 1998.0
Contract Killer Wei Tung 5.697164484451719 1998.0
Pecker John Waters 5.702479212521397 1998.0
Zero Effect Jake Kasdan 5.715557049333645 1998.0
Meeting People Is Easy Grant Gee 5.718126477541372 1998.0
All I Wanna Do Sarah Kernochan 5.7192360601614105 1998.0
Belly Hype Williams 5.719684305056645 1998.0
Genesis Nacho Cerdà 5.725797872340425 1998.0
Autumn Tale Éric Rohmer 5.725865776369398 1998.0
The Visitors II: The Corridors of Time Jean-Marie Poiré 5.727436089313162 1998.0
Vampires John Carpenter 5.735743068987747 1998.0
The Power of Kangwon Province Hong Sang-soo 5.744285158277114 1998.0
The Temptations Allan Arkush 5.747259009986974 1998.0
Kite Yasuomi Umetsu 5.747643710339679 1998.0
Snake Eyes Brian De Palma 5.747857080572026 1998.0
Behind the Planet of the Apes Kevin Burns 5.752410239361702 1998.0
Hornblower: The Even Chance Andrew Grieve 5.759131205673759 1998.0
Hilary and Jackie Anand Tucker 5.760615370506238 1998.0
High Art Lisa Cholodenko 5.761428231184503 1998.0
Sitcom François Ozon 5.763156314344545 1998.0
Sphere Barry Levinson 5.763157705577918 1998.0
Next Stop Paradise Lucian Pintilie 5.7646593211752775 1998.0
Depeche Mode: The Videos 86-98  5.7646593211752775 1998.0
West Beyrouth Ziad Doueiri 5.7728922872340425 1998.0
The General John Boorman 5.773147313379011 1998.0
The Cruise Bennett Miller 5.774783523008411 1998.0
BASEketball David Zucker 5.778463197239794 1998.0
Peculiarities of the National Fishing Aleksandr Rogozhkin 5.779147913256956 1998.0
Love Is the Devil: Study for a Portrait of Francis Bacon John Maybury 5.7799202127659575 1998.0
Paulie John Roberts 5.780641453299675 1998.0
A Night at the Roxbury John Fortenberry 5.805129499955112 1998.0
Mighty Joe Young Ron Underwood 5.807471981437702 1998.0
Bullet Ballet Shinya Tsukamoto 5.808317604667122 1998.0
Eternity and a Day Theo Angelopoulos 5.808483499782891 1998.0
April Story Shunji Iwai 5.808483499782891 1998.0
Clay Pigeons David Dobkin 5.812374209315699 1998.0
The Acid House Paul McGuigan 5.819348404255319 1998.0
Celebrity Woody Allen 5.825106023576769 1998.0
Samurai Fiction Hiroyuki Nakano 5.834258329987956 1998.0
The Dreamlife of Angels Erick Zonca 5.834258329987956 1998.0
The Girl of Your Dreams Fernando Trueba 5.834313829787233 1998.0
Besieged Bernardo Bertolucci 5.8390957446808525 1998.0
The Last Days James Moll 5.8403486997635925 1998.0
The Replacement Killers Antoine Fuqua 5.841200561284428 1998.0
Another Day in Paradise Larry Clark 5.859119061498108 1998.0
The Waterboy Frank Coraci 5.859560596203887 1998.0
Deep Rising Stephen Sommers 5.86376679731243 1998.0
I Married a Strange Person! Bill Plympton 5.864163648709821 1998.0
Sombre Philippe Grandrieux 5.866265715667312 1998.0
Babylon 5: Thirdspace Jesús Salvador Treviño 5.8664282633697535 1998.0
Divine Trash Steve Yeager 5.8690769060283685 1998.0
The Apple Samira Makhmalbaf 5.86970798957881 1998.0
Smoke Signals Chris Eyre 5.871928191489363 1998.0
Deep Impact Mimi Leder 5.87515546020924 1998.0
Primary Colors Mike Nichols 5.878149177949709 1998.0
The Hole Tsai Ming-liang 5.892464539007093 1998.0
The Wounds Srđan Dragojević 5.892464539007093 1998.0
The Big Hit Kirk Wong 5.900787016934433 1998.0
A Civil Action Steven Zaillian 5.902133050603795 1998.0
Can't Hardly Wait Harry Elfont 5.913475741970457 1998.0
Dil Se.. Mani Ratnam 5.923682679521277 1998.0
Cats David Mallet 5.928068360914105 1998.0
Return to Paradise Joseph Ruben 5.928209219858156 1998.0
Mercury Rising Harold Becker 5.936713182125579 1998.0
Bulworth Warren Beatty 5.942729618706551 1998.0
Aprile Nanni Moretti 5.9493980963045905 1998.0
Everything's Gonna Be Great Ömer Vargı 5.959036771507863 1998.0
Slums of Beverly Hills Tamara Jenkins 5.9639461436170205 1998.0
Antz Eric Darnell 5.980897189291043 1998.0
Soldier Paul W.S. Anderson 5.987799584250428 1998.0
Flowers of Shanghai Hou Hsiao-hsien 5.9924645390070905 1998.0
Simon Birch Mark Steven Johnson 6.007076913670825 1998.0
The Interview Craig Monahan 6.00769688790092 1998.0
The Emperor and the Assassin Chen Kaige 6.013503613006824 1998.0
Jerry Seinfeld: I'm Telling You for the Last Time Marty Callner 6.013503613006824 1998.0
Hitman Hart: Wrestling with Shadows Paul Jay 6.013503613006824 1998.0
The Bird People in China Takashi Miike 6.0182900244157675 1998.0
Get Real Simon Shore 6.021446143617022 1998.0
The Siege Edward Zwick 6.026049725983236 1998.0
Halloweentown Duwayne Dunham 6.026634026434557 1998.0
Apt Pupil Bryan Singer 6.0389935541347874 1998.0
Christmas in August Hur Jin-Ho   6.039179472025218 1998.0
Playing by Heart Willard Carroll 6.048996327254306 1998.0
Very Bad Things Peter Berg 6.049381066771254 1998.0
The Thing: Terror Takes Shape Michael Matessino 6.070811170212767 1998.0
Dirty Work Bob Saget 6.0754028860332845 1998.0
Savior Predrag Antonijević 6.111182679521277 1998.0
My Name Is Joe Ken Loach 6.111182679521277 1998.0
U.S. Marshals Stuart Baird 6.121807874376464 1998.0
Little Voice Mark Herman 6.122842180302698 1998.0
The Faculty Robert Rodriguez 6.1389078345595545 1998.0
Small Soldiers Joe Dante 6.141320810573361 1998.0
Great Expectations Alfonso Cuarón 6.162142719815433 1998.0
The Last Days of Disco Whit Stillman 6.1754787234042565 1998.0
Still Crazy Brian Gibson 6.195938449848024 1998.0
A Perfect Murder Andrew Davis 6.198354060533413 1998.0
The Barber of Siberia Nikita Mikhalkov 6.204711813274055 1998.0
Practical Magic Griffin Dunne 6.205785095272485 1998.0
Last Night Don McKellar 6.212048572228444 1998.0
Half Baked Tamra Davis 6.215493885464307 1998.0
Star Trek: Insurrection Jonathan Frakes 6.217427211646137 1998.0
Wild Things John McNaughton 6.226207958055954 1998.0
The Count of Monte Cristo Josée Dayan 6.2514461436170174 1998.0
Lethal Weapon 4 Richard Donner 6.255833159717701 1998.0
You've Got Mail Nora Ephron 6.2596372388024175 1998.0
The Man in the Iron Mask Randall Wallace 6.2668037640196514 1998.0
Croupier Mike Hodges 6.267962534690103 1998.0
The Mask of Zorro Martin Campbell 6.271039880810082 1998.0
The Quiet Family Kim Jee-woon 6.273829048463358 1998.0
Dangerous Beauty Marshall Herskovitz 6.2991031009506555 1998.0
Detective Conan: The Fourteenth Target Kenji Kodama 6.317164484451719 1998.0
Gods and Monsters Bill Condon 6.346287961447534 1998.0
The Idiots Lars von Trier 6.350110503446209 1998.0
The Mighty Peter Chelsom 6.364135947550716 1998.0
The Batman Superman Movie: World's Finest Toshihiko Masuda 6.380860130560928 1998.0
Who Am I? Jackie Chan 6.382702776945361 1998.0
Out of Sight Steven Soderbergh 6.3850286635103535 1998.0
Lovers of the Arctic Circle Julio Médem 6.3889403538130525 1998.0
23 Hans-Christian Schmid 6.396841016548463 1998.0
Batman & Mr. Freeze: SubZero Boyd Kirkland 6.4045236095558025 1998.0
He Got Game Spike Lee 6.4160896656534945 1998.0
City of Angels Brad Silberling 6.4250274326737085 1998.0
The Wedding Singer Frank Coraci 6.432783215500567 1998.0
Babylon 5: In the Beginning Michael Vejar 6.434761623325452 1998.0
Taxi Gérard Pirès 6.437668156234538 1998.0
Gia Michael Cristofer 6.439872730310795 1998.0
Notre Dame de Paris Gilles Amado 6.460912975235437 1998.0
After Life Hirokazu Koreeda 6.465662993920972 1998.0
There's Something About Mary Bobby Farrelly 6.474025252264908 1998.0
Blade Stephen Norrington 6.478395814352323 1998.0
Fallen Gregory Hoblit 6.4785188753001615 1998.0
Armageddon Michael Bay 6.483345899607519 1998.0
Sliding Doors Peter Howitt 6.483707767561209 1998.0
Thursday Skip Woods 6.4967705167173255 1998.0
The X Files Rob Bowman 6.512506033061175 1998.0
Train of Life Radu Mihaileanu 6.514646885413997 1998.0
Velvet Goldmine Todd Haynes 6.533218775904947 1998.0
The Horse Whisperer Robert Redford 6.5498963489104565 1998.0
Pokémon: The First Movie: Mewtwo Strikes Back Michael Haigney 6.566080030968631 1998.0
Waking Ned Kirk Jones 6.609846145046903 1998.0
Scooby-Doo on Zombie Island Hiroshi Aoyama 6.63004864520822 1998.0
From the Earth to the Moon Michael Grossman 6.637962155563308 1998.0
A Simple Plan Sam Raimi 6.650069431368857 1998.0
Quest for Camelot Frederik Du Chau 6.652261804777902 1998.0
The Lion King 2: Simba's Pride Darrell Rooney 6.653522630018113 1998.0
Enemy of the State Tony Scott 6.6618106775475505 1998.0
Lucky and Zorba Enzo D'Alò 6.674926704640644 1998.0
Ever After: A Cinderella Story Andy Tennant 6.680170861149801 1998.0
The Red Violin François Girard 6.70967420212766 1998.0
The Negotiator F. Gary Gray 6.715470846320637 1998.0
What Dreams May Come Vincent Ward 6.7158727281923305 1998.0
Stepmom Chris Columbus 6.724036422085239 1998.0
Ronin John Frankenheimer 6.7247031084955475 1998.0
Show Me Love Lukas Moodysson 6.725536636031724 1998.0
The Parent Trap Nancy Meyers 6.733892517421372 1998.0
Ringu Hideo Nakata 6.736277366635831 1998.0
Shakespeare in Love John Madden 6.73870172227409 1998.0
SLC Punk James Merendino 6.7524663397606375 1998.0
Rush Hour Brett Ratner 6.758817448789265 1998.0
More Mark Osborne 6.7689461436170175 1998.0
Les Misérables Bille August 6.770394226507093 1998.0
A Bug's Life John Lasseter 6.778009814204375 1998.0
Rounders John Dahl 6.783777142982228 1998.0
The Prince of Egypt Brenda Chapman 6.8359588338881645 1998.0
Meet Joe Black Martin Brest 6.853280141843972 1998.0
Buffalo '66 Vincent Gallo 6.877853582074213 1998.0
Kuch Kuch Hota Hai Karan Johar 6.899341339760637 1998.0
I Stand Alone Gaspar Noé 6.926236503651952 1998.0
Patch Adams Tom Shadyac 6.9333136876273525 1998.0
Kirikou and the Sorceress Michel Ocelot 6.947407922308187 1998.0
Pleasantville Gary Ross 6.981267713863717 1998.0
Pi Darren Aronofsky 7.000339671636789 1998.0
Elizabeth Shekhar Kapur 7.024975568213548 1998.0
Following Christopher Nolan 7.032451486154175 1998.0
Happiness Todd Solondz 7.083257290902423 1998.0
Run Lola Run Tom Tykwer 7.105679903096693 1998.0
Central Station Walter Salles 7.119255637660848 1998.0
The Thin Red Line Terrence Malick 7.119463395518554 1998.0
Dark City Alex Proyas 7.123614766883576 1998.0
Fear and Loathing in Las Vegas Terry Gilliam 7.1508227813343375 1998.0
That's Life Massimo Venier 7.180977972984903 1998.0
The Celebration Thomas Vinterberg 7.362563272134522 1998.0
Rushmore Wes Anderson 7.393659324328301 1998.0
The Dinner Game Francis Veber 7.5110963690504775 1998.0
Mulan Tony Bancroft 7.562201361341508 1998.0
The Big Lebowski Joel Coen 7.771316103916128 1998.0
The Truman Show Peter Weir 7.781616147665052 1998.0
Saving Private Ryan Steven Spielberg 7.8825420975283365 1998.0
The Legend of 1900 Giuseppe Tornatore 7.93483139471878 1998.0
American History X Tony Kaye 8.168087382405501 1998.0
Total execution time in seconds: 0.0025407
 */
