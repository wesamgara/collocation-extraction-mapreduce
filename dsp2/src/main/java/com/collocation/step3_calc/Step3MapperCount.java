package com.collocation.step3_calc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.collocation.DecadeWordKey;

public class Step3MapperCount extends Mapper<LongWritable, Text, DecadeWordKey, Text> {
    
    // Copy the same Stop Words setup here as Step 2
    private Set<String> stopWords;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        stopWords = new HashSet<>();

        String[] hebrewStopWords = {
            "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי",
            "מהם", "מה", "מ", "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה",
            "כלי", "כל", "כי", "יש", "ימים", "יותר", "יד", "י", "זה", "ז", "ועל", "ומי",
            "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו", "היה",
            "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את",
            "אשר", "אם", "אלה", "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז",
            "אותו", "־", "^", "?", ";", ":", "1", ".", "-", "*", "\"", "!", "שלשה", "בעל",
            "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל",
            "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי",
            "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק", "שני",
            "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם",
            "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן",
            "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או", "אבל",
            "א"
        };

        // 2. English Stop Words (You should add the English list here too)
        String[] englishStopWords = {
            "a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost",
            "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst",
            "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere",
            "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming",
            "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond",
            "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con",
            "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during",
            "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even",
            "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill",
            "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from",
            "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her",
            "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his",
            "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it",
            "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may",
            "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much",
            "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no",
            "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on",
            "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves",
            "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see",
            "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since",
            "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes",
            "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them",
            "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon",
            "these", "they", "thick", "thin", "third", "this", "those", "though", "three", "through", "throughout",
            "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un",
            "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever",
            "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon",
            "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why",
            "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves"
        };

            // Add stop words to the HashSet
            for (String word : hebrewStopWords) {
                stopWords.add(word);
            }
            for (String word : englishStopWords) {
                stopWords.add(word);
            }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");
        if (parts.length < 3) return;

        String word = parts[0]; // This is our potential "Word2"
        String countStr = parts[2];

        // Filter stop words just to be safe (though usually 2-grams are already filtered)
        if (stopWords.contains(word.toLowerCase())) return; 

        try {
            int year = Integer.parseInt(parts[1]);
            int decade = (year / 10) * 10;

            // KEY: Group by Decade + Word. 
            // Tag = 0 (Count, arrives 1st)
            DecadeWordKey outKey = new DecadeWordKey(String.valueOf(decade), word, 0);
            
            // VALUE: "c1:500" (This is C2, the count of the second word)
            Text outValue = new Text("c1:" + countStr);

            context.write(outKey, outValue);

        } catch (NumberFormatException e) { }
    }
}