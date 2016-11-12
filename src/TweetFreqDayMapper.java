import java.io.*;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.lang.NumberFormatException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TweetFreqDayMapper extends Mapper<Object, Text, Text, LongWritable> { 

    // Long used for convenience and to avoid casting during the reducer
    // summation. This helps avoid potential integer overflows with very large
    // datasets.
	private LongWritable value = new LongWritable(1);
    private Text feature = new Text();

    private String matchTweetToTeam(String tweet) {
        // Create list of hash tags found in the tweet
        // Code adapted from: http://stackoverflow.com/questions/10432543/extract-hash-tag-from-string
        Pattern hashtag_pattern = Pattern.compile("#(\\w+)");
        Matcher mat = hashtag_pattern.matcher(tweet);

        List<String> tags=new ArrayList<String>();
        while (mat.find()) {
            tags.add(mat.group(1));
        }
        
        Pattern goteam_pattern = Pattern.compile("(go|team)", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);

        String out = "";
        // Search for strings "go" and "team" using regex
        // For each hashtag found...
        for (String tag : tags) {
            // If either are found search for country code
            mat = goteam_pattern.matcher(tag);
            if (mat.find()) {   
                out = CountryCodes.findCountryCode(tweet);
            }
            else
            {
                // Else search country names/denonyms for team
                out = Denonyms.findDenonym(tweet);
            }
        }
        System.out.println(out);
        System.out.println(tweet);
        return out;
    }

    public static String strJoin(String[] aArr, String sSep) {
        // Method taken from: http://stackoverflow.com/questions/1978933/a-quick-and-easy-way-to-join-array-elements-with-a-separator-the-opposite-of-sp
        StringBuilder sbStr = new StringBuilder();
        for (int i = 0, il = aArr.length; i < il; i++) {
            if (i > 0)
                sbStr.append(sSep);
            sbStr.append(aArr[i]);
        }
        return sbStr.toString();
    }

    public void map(Object key, Text input_val, Context context) throws IOException, InterruptedException {
   
        // For each input tweet...
        // Seperate raw line into an array of it's elements (seperated in the
        // file by ';')
    	String[] line = input_val.toString().split(";");
        String featureString = "";

        // Filter out invalid entries that aren't complete
        if(line.length == 4) {
            // Get a date object for the tweet date
            try {
                // Maintained to raise error if data is malformed
                Long.parseLong(line[0]);

                featureString = matchTweetToTeam(line[2]);
                if(featureString.length() > 0) {
                    feature.set(featureString);
                    context.write(feature,value);
                }

            }
            // If data is not formed correctly then skip it
            catch (NumberFormatException e) {
                return;
            }

        }
    }
}

class CountryCodes {
    private static String[] country = {
        "United States",
        "Great Britain",
        "China",
        "Russia",
        "Germany",
        "Japan",
        "France",
        "South Korea",
        "Italy",
        "Australia",
        "Netherlands",
        "Hungary",
        "Brazil",
        "Spain",
        "Kenya",
        "Jamaica",
        "Croatia",
        "Cuba",
        "New Zealand",
        "Canada",
        "Uzbekistan",
        "Kazakhstan",
        "Colombia",
        "Switzerland",
        "Iran",
        "Greece",
        "Argentina",
        "Denmark",
        "Sweden",
        "South Africa",
        "Ukraine",
        "Serbia",
        "Poland",
        "North Korea",
        "Belgium",
        "Thailand",
        "Slovakia",
        "Georgia",
        "Azerbaijan",
        "Belarus",
        "Turkey",
        "Armenia",
        "Czech Republic",
        "Ethiopia",
        "Slovenia",
        "Indonesia",
        "Romania",
        "Bahrain",
        "Vietnam",
        "Chinese Taipei",
        "Bahamas",
        "Ivory Coast",
        "Fiji",
        "Jordan",
        "Kosovo",
        "Puerto Rico",
        "Singapore",
        "Tajikistan",
        "Malaysia",
        "Mexico",
        "Algeria",
        "Ireland",
        "Lithuania",
        "Bulgaria",
        "Venezuela",
        "India",
        "Mongolia",
        "Burundi",
        "Grenada",
        "Niger",
        "Philippines",
        "Qatar",
        "Norway",
        "Egypt",
        "Tunisia",
        "Israel",
        "Austria",
        "Dominican Republic",
        "Estonia",
        "Finland",
        "Morocco",
        "Moldova",
        "Nigeria",
        "Portugal",
        "Trinidad and Tobago",
        "United Arab Emirates"
    };

    private static Pattern[] code = {
        Pattern.compile("US"),
        Pattern.compile("GB|GBR"),
        Pattern.compile("CN|CHN"),
        Pattern.compile("RU"),
        Pattern.compile("DE|DEU"),
        Pattern.compile("JP|JPN"),
        Pattern.compile("FR|FRA"),
        Pattern.compile("KR|KOR"),
        Pattern.compile("IT|ITA"),
        Pattern.compile("AU|AUS"),
        Pattern.compile("NL|NLD"),
        Pattern.compile("HU|HUN"),
        Pattern.compile("BR|BRA"),
        Pattern.compile("ES|ESP"),
        Pattern.compile("KE|KEN"),
        Pattern.compile("JM|JAM"),
        Pattern.compile("HR|HRV"),
        Pattern.compile("CU|CUB"),
        Pattern.compile("NZ|NZL"),
        Pattern.compile("CA"),
        Pattern.compile("UZ|UZB"),
        Pattern.compile("KZ"),
        Pattern.compile("CO|COL"),
        Pattern.compile("CH|CHE"),
        Pattern.compile("IR|IRN"),
        Pattern.compile("GR|GRC"),
        Pattern.compile("AR|ARG"),
        Pattern.compile("DK|DNK"),
        Pattern.compile("SE|SWE"),
        Pattern.compile("ZA|ZAF"),
        Pattern.compile("UA|UKR"),
        Pattern.compile("RS|SRB"),
        Pattern.compile("PL|POL"),
        Pattern.compile("KP|PRK"),
        Pattern.compile("BE|BEL"),
        Pattern.compile("TH|THA"),
        Pattern.compile("SK|SVK"),
        Pattern.compile("GE|GEO"),
        Pattern.compile("AZ|AZE"),
        Pattern.compile("BY|BLR"),
        Pattern.compile("TR|TUR"),
        Pattern.compile("AM|ARM"),
        Pattern.compile("CZ|CZE"),
        Pattern.compile("ET|ETH"),
        Pattern.compile("SI|SVN"),
        Pattern.compile("ID|IDN"),
        Pattern.compile("RO|ROU"),
        Pattern.compile("BH|BHR"),
        Pattern.compile("VN|VNM"),
        Pattern.compile("TW|TWN"),
        Pattern.compile("BS|BHS"),
        Pattern.compile("CI|CIV"),
        Pattern.compile("FJ|FJI"),
        Pattern.compile("JO|JOR"),
        Pattern.compile("XK|XKX"),
        Pattern.compile("PR"),
        Pattern.compile("SG|SGP"),
        Pattern.compile("TJ|TJK"),
        Pattern.compile("MY|MYS"),
        Pattern.compile("MX|MEX"),
        Pattern.compile("DZ|DZA"),
        Pattern.compile("IE|IRL"),
        Pattern.compile("LT|LTU"),
        Pattern.compile("BG|BGR"),
        Pattern.compile("VE|VEN"),
        Pattern.compile("IN|IND"),
        Pattern.compile("MN|MNG"),
        Pattern.compile("BI|BDI"),
        Pattern.compile("GD|GRD"),
        Pattern.compile("NE|NER"),
        Pattern.compile("PH|PHL"),
        Pattern.compile("QA|QAT"),
        Pattern.compile("NO|NOR"),
        Pattern.compile("EG|EGY"),
        Pattern.compile("TN|TUN"),
        Pattern.compile("IL|ISR"),
        Pattern.compile("AT|AUT"),
        Pattern.compile("DO|DOM"),
        Pattern.compile("EE|EST"),
        Pattern.compile("FI|FIN"),
        Pattern.compile("MA|MAR"),
        Pattern.compile("MD|MDA"),
        Pattern.compile("NG|NGA"),
        Pattern.compile("PT|PRT"),
        Pattern.compile("TT|TTO"),
        Pattern.compile("AE|ARE"),
    };
    public static String findCountryCode(String tweet) {
        int i = 0;
        while(i < code.length) {
            Matcher mat = code[i].matcher(tweet);
            if(mat.find()) {
                return country[i];
            }
            i++;
        }
        return "";
    }
}

class Denonyms {
    private static Pattern[] den = {
        Pattern.compile("(United States|US|American|Americans|Yankees|Yanks)"),
        Pattern.compile("(US Virgin Island|US Virgin Islanders)"),
        Pattern.compile("(British|UK|Britons|British)"),
        Pattern.compile("(Chinese Taipei|Chinese)"),
        Pattern.compile("(Taiwanese)"),
        Pattern.compile("(Russian|Russians)"),
        Pattern.compile("(German|Germans)"),
        Pattern.compile("(Japanese|Japanese)"),
        Pattern.compile("(French|French|Frenchmen|Frenchwomen)"),
        Pattern.compile("(South Korean)"),
        Pattern.compile("(Italian|Italians)"),
        Pattern.compile("(Australian|Australians|Aussies)"),
        Pattern.compile("(Dutch|Netherlandic|Dutch|Dutchmen|Dutchwomen|Netherlanders)"),
        Pattern.compile("(Hungarian|Magyar|Hungarians|Magyars)"),
        Pattern.compile("(Brazilian|Brazilians)"),
        Pattern.compile("(Spanish|Spaniards)"),
        Pattern.compile("(Kenyan|Kenyans)"),
        Pattern.compile("(Jamaican|Jamaicans)"),
        Pattern.compile("(Croatian|Croatians|Croats)"),
        Pattern.compile("(Cuban|Cubans)"),
        Pattern.compile("(New Zealand|NZ|New Zealanders|Kiwis)"),
        Pattern.compile("(Canadian|Canadians|Canucks)"),
        Pattern.compile("(Uzbekistani|Uzbek|Uzbekistanis|Uzbeks)"),
        Pattern.compile("(Kazakhstani|Kazakh|Kazakhstanis|Kazakhs)"),
        Pattern.compile("(Colombian|Colombians)"),
        Pattern.compile("(Swiss|Swiss)"),
        Pattern.compile("(Iranian|Persian|Iranians|Persians)"),
        Pattern.compile("(Greek|Hellenic|Greeks|Hellenes)"),
        Pattern.compile("(Argentine|Argentines)"),
        Pattern.compile("(Danish|Danes)"),
        Pattern.compile("(Swedish|Swedes)"),
        Pattern.compile("(South African|South Africans)"),
        Pattern.compile("(Ukrainian|Ukrainians)"),
        Pattern.compile("(Serbian|Serbs|Serbians)"),
        Pattern.compile("(Polish|Poles)"),
        Pattern.compile("(North Korean|Koreans)"),
        Pattern.compile("(Belgian|Belgians)"),
        Pattern.compile("(Thai|Thai)"),
        Pattern.compile("(Slovak|Slovaks)"),
        Pattern.compile("(Georgian|Georgians)"),
        Pattern.compile("(Azerbaijani|Azerbaijanis)"),
        Pattern.compile("(Belarusian|Belarusians)"),
        Pattern.compile("(Turkish|Turks)"),
        Pattern.compile("(Armenian|Armenians)"),
        Pattern.compile("(Czech|Czechs)"),
        Pattern.compile("(Ethiopian|Ethiopians Habesha)"),
        Pattern.compile("(Slovenian|Slovene|Slovenes|Slovenians)"),
        Pattern.compile("(Indonesian|Indonesians)"),
        Pattern.compile("(Romanian|Romanians)"),
        Pattern.compile("(Bahraini|Bahrainis)"),
        Pattern.compile("(Vietnamese|Vietnamese)"),
        Pattern.compile("(Chinese Taipei|Chinese)"),
        Pattern.compile("(Bahamian|Bahamians)"),
        Pattern.compile("(Ivorian|Ivorians)"),
        Pattern.compile("(Fijian|Fijians)"),
        Pattern.compile("(Jordanian|Jordanians)"),
        Pattern.compile("(Kosovar|Kosovan|Kosovars)"),
        Pattern.compile("(Puerto Rican|Puerto Ricans|Boricuas)"),
        Pattern.compile("(Singapore|Singaporean|Singaporeans)"),
        Pattern.compile("(Tajikistani|Tajikistanis|Tajiks)"),
        Pattern.compile("(Malaysian|Malaysians)"),
        Pattern.compile("(Mexican|Mexicans)"),
        Pattern.compile("(Algerian|Algerians)"),
        Pattern.compile("(Irish|Irish|Irishmen|Irishwomen)"),
        Pattern.compile("(Lithuanian|Lithuanians)"),
        Pattern.compile("(Bulgarian|Bulgarians)"),
        Pattern.compile("(Venezuelan|Venezuelans)"),
        Pattern.compile("(Indian|Indians)"),
        Pattern.compile("(Mongolian|Mongolians|Mongols)"),
        Pattern.compile("(Burundian|Burundians|Barundi)"),
        Pattern.compile("(Grenadian|Grenadians)"),
        Pattern.compile("(Nigerien|Nigeriens)"),
        Pattern.compile("(Filipino|Philippine|Filipinos|Filipinas|Pinoys)"),
        Pattern.compile("(Qatari|Qataris)"),
        Pattern.compile("(Norwegian|Norwegians)"),
        Pattern.compile("(Egyptian|Egyptians)"),
        Pattern.compile("(Tunisian|Tunisians)"),
        Pattern.compile("(Israeli|Israelis)"),
        Pattern.compile("(Austrian|Austrians)"),
        Pattern.compile("(Dominican|Dominicans|Quisqueyanos)"),
        Pattern.compile("(Estonian|Estonians)"),
        Pattern.compile("(Finnish|Finns)"),
        Pattern.compile("(Moroccan|Moroccans)"),
        Pattern.compile("(Moldovan|Moldovans)"),
        Pattern.compile("(Nigerian|Nigerians)"),
        Pattern.compile("(Portuguese|Portuguese)"),
        Pattern.compile("(Trinidadian|Tobagonian|Trinidadians|Tobagonians|Trinis|Trinbagonians)"),
        Pattern.compile("(Emirati|Emirian|Emiri|Emiratis|Emirians|Emiri)"),
    };

    private static String[] country = {
        "United States",
        "Virgin Islands",
        "Great Britain",
        "China",
        "China",
        "Russia",
        "Germany",
        "Japan",
        "France",
        "South Korea",
        "Italy",
        "Australia",
        "Netherlands",
        "Hungary",
        "Brazil",
        "Spain",
        "Kenya",
        "Jamaica",
        "Croatia",
        "Cuba",
        "New Zealand",
        "Canada",
        "Uzbekistan",
        "Kazakhstan",
        "Colombia",
        "Switzerland",
        "Iran",
        "Greece",
        "Argentina",
        "Denmark",
        "Sweden",
        "South Africa",
        "Ukraine",
        "Serbia",
        "Poland",
        "North Korea",
        "Belgium",
        "Thailand",
        "Slovakia",
        "Georgia",
        "Azerbaijan",
        "Belarus",
        "Turkey",
        "Armenia",
        "Czech Republic",
        "Ethiopia",
        "Slovenia",
        "Indonesia",
        "Romania",
        "Bahrain",
        "Vietnam",
        "China",
        "Bahamas",
        "Ivory Coast",
        "Fiji",
        "Jordan",
        "Kosovo",
        "Puerto Rico",
        "Singapore",
        "Tajikistan",
        "Malaysia",
        "Mexico",
        "Algeria",
        "Ireland",
        "Lithuania",
        "Bulgaria",
        "Venezuela",
        "India",
        "Mongolia",
        "Burundi",
        "Grenada",
        "Niger",
        "Philippines",
        "Qatar",
        "Norway",
        "Egypt",
        "Tunisia",
        "Israel",
        "Austria",
        "Dominican Republic",
        "Estonia",
        "Finland",
        "Morocco",
        "Moldova",
        "Nigeria",
        "Portugal",
        "Trinidad and Tobago",
        "United Arab Emirates"
    };
    public static String findDenonym(String tweet) {
        int i = 0;
        while(i < den.length) {
            Matcher mat = den[i].matcher(tweet);
            if(mat.find()) {
                return country[i];
            }
            i++;
        }
        return "";
    }
}
