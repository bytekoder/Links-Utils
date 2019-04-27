package cs.linksutil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Link implements Serializable {

    private String url;
    private Long timestamp;
    private List<String> tags;

    private Link(String url, Long timestamp, List<String> tags) {

        this.url = url;
        this.timestamp = timestamp;
        this.tags = tags;
    }

    public static Link parse(String line) {

        int urlTokenEnd = line.indexOf(URL_TOKEN) + URL_TOKEN.length();
        int urlStart = line.indexOf(QUOTE, urlTokenEnd) + 1;
        assert urlStart > urlTokenEnd : String.format("urlTokenEnd: %d, urlStart: %d", urlTokenEnd, urlStart);
        int urlEnd = line.indexOf(QUOTE, urlStart);
        assert urlEnd > urlStart;
        String url = line.substring(urlStart, urlEnd);
        int timestampTokenEnd = line.indexOf(TIMESTAMP_TOKEN, urlEnd) + TIMESTAMP_TOKEN.length();
        int timestampStart = line.indexOf(SPACE, timestampTokenEnd);
        // Get past consecutive spaces
        while (line.charAt(timestampStart) == SPACE) {
            timestampStart++;
        }
        int timestampEnd = line.indexOf(COMMA, timestampStart);
        long timestamp = Long.parseLong(line.substring(timestampStart, timestampEnd));
        int tagsTokenEnd = line.indexOf(TAGS_TOKEN, timestampEnd) + TAGS_TOKEN.length();
        int startQuote;
        int endQuote = tagsTokenEnd;
        List<String> tags = new ArrayList<String>();
        while ((startQuote = line.indexOf(QUOTE, endQuote + 1) + 1) != 0) {
            endQuote = line.indexOf(QUOTE, startQuote);
            if (endQuote < 0 || startQuote < 0) {
                return null;
            }
            String tag = line.substring(startQuote, endQuote);
            tags.add(tag);
        }
        return new Link(url, timestamp, tags);
    }

    public String url() {

        return url;
    }

    public List<String> tags() {

        return tags;
    }

    public String tagString() {

        String stringOfTags = "";
        for (String tag : tags) {
            stringOfTags += tag + ":";
        }
        return stringOfTags;
    }

    public long timestamp() {

        return timestamp;
    }

    private static final String URL_TOKEN = "\"url\"";
    private static final String TIMESTAMP_TOKEN = "\"timestamp\"";
    private static final String TAGS_TOKEN = "\"tags\"";
    private static final char QUOTE = '"';
    private static final char SPACE = ' ';
    private static final char COMMA = ',';

}

