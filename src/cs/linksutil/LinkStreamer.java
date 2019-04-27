package cs.linksutil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author bhavanishekhawat
 */
public class LinkStreamer {

    private static final String MESSAGE = "Usage: input-directory output-file dd-MM-yyyy dd-MM-yyyy";
    private static final String SEPERATOR = "  ";

    private static String inputDir;
    private static String outFile;
    private static final Map<String, List<String>> map = new HashMap();
    private static final SortedMap<String, Integer> countMap = new TreeMap();
    private static final List<Path> files = new ArrayList<>();

    public static void main(String[] args) {

        inputDir = args[0].trim();
        outFile = args[1].trim();

        // Looks at the provided arguments and falls through the appropriate switch case
        switch (args.length) {
            case 1:
                System.out.println("Output directory missing");
                System.out.println(MESSAGE);
                break;
            case 2:
                getAllFiles();
                streamAllContent();
                writeToFile();
                break;
            case 3:
                System.out.println("date-range missing");
                System.out.println(MESSAGE);
                break;
            case 4:
                getAllFiles();
                streamAllContentByDate(args[2], args[3]);
                writeToFileWithCount();
                break;
            default:
                System.out.println("No args specified");
                System.out.println(MESSAGE);
                break;
        }

    }

    /**
     * Gets all the file names from the supplied folder by doing a walk through.
     */
    private static void getAllFiles() {

        try {
            try (Stream<Path> paths = Files.walk(Paths.get(inputDir))) {
                paths.forEach(filePath -> {
                    // Make sure files are not sym links
                    if (Files.isRegularFile(filePath)) {
                        // If found! then add it to the list where the file names are maintained
                        files.add(filePath);
                    }
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Streams all the content while applying the timestamp filter
     *
     * @param begin The date where to start looking for URLs
     * @param end   The date where to stop looking for URLs
     */
    private static void streamAllContentByDate(String begin, String end) {

        Date beginDate = null, endDate = null;
        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
        format.setTimeZone(TimeZone.getTimeZone("EST"));

        try {
            beginDate = format.parse(begin);
            endDate = format.parse(end);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // Make sure the dates are not empty
        assert beginDate != null;
        assert endDate != null;

        // Convert the dates into their timestamps since we have timestamps in the files
        Long startSeconds = beginDate.getTime() / 1000;
        Long stopSeconds = endDate.getTime() / 1000;

        // Temporary map to hold file-specific results. Therefore, no need to make this sorted.
        Map<String, Integer> temporaryMap = new HashMap<>();

        // Loop through the list containing the file names
        for (Path filePath : files) {
            try {
                /**
                 * 1. Grab the file first
                 * 2. Apply the date filter
                 * 3. Fetch the links as well as group them at the same time
                 * 4. Create an anonymous map that will collect all the links with their count
                 * 5. Store the result in countMap
                 */
                temporaryMap = Files.lines(Paths.get(filePath.toUri()))
                                    .filter(s -> Link.parse(s).timestamp() > startSeconds
                                                    && Link.parse(s).timestamp() < stopSeconds)
                                    .collect(Collectors.groupingBy(x -> Link.parse(x).url())).entrySet().stream()
                                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size()));
            } catch (IOException e) {
                e.printStackTrace();
            }
            // Put the results coming from each in this permanent map
            countMap.putAll(temporaryMap);
        }

    }

    /**
     * Streams all the content in form of <p>link &nbsp&nbsp link-tags</p>
     */
    private static void streamAllContent() {

        // Loop through the list to grab the file names
        for (Path filePath : files) {
            /**
             * 1. Grab the file first
             * 2. Fetch the links as well as tags at the same time
             * 3. Store each value result in the map
             */
            try {
                Files.lines(Paths.get(filePath.toUri()))
                     .forEach(s -> map.put(Link.parse(s).url(), Link.parse(s).tags()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * I hate how I wrote the next two methods. I should have generified them.
     * Writes everything to file in form of <p>link &nbsp&nbsp link-tags</p>
     */
    private static void writeToFile() {

        // Convert the outFile into a path so that the System can create the file
        Path path = Paths.get(outFile);

        // Loop over the existing hashmap and grab the keys and values
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            try {
                // Stream the output and write it to file
                Files.write(path, () -> map.entrySet().stream().<CharSequence>map(
                                e -> e.getKey() + SEPERATOR + e.getValue()).iterator());

            } catch (IOException e) {
                e.printStackTrace();
            }
            // Customary SOUT
            System.out.println(entry.getKey() + SEPERATOR + entry.getValue().toString());

        }
    }

    /**
     * Writes to the given output file in the format of <p>link &nbsp&nbsp link-count</p>
     */
    private static void writeToFileWithCount() {

        // Convert the outFile into a path so that the System can create the file
        Path path = Paths.get(outFile);

        // Loop over the existing hashmap and grab the keys and values
        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            try {
                // Stream the output and write it to file
                Files.write(path, () -> countMap.entrySet().stream().<CharSequence>map(
                                e -> e.getKey() + SEPERATOR + e.getValue()).iterator());

            } catch (IOException e) {
                e.printStackTrace();
            }

            // Customary SOUT
            System.out.println(entry.getKey() + SEPERATOR + entry.getValue());

        }
    }
}
