package org.aksw.linkedspending;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Properties;

import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.feed.synd.SyndFeedImpl;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedOutput;
import org.aksw.linkedspending.tools.PropertiesLoader;

/**Class for writing a newsfeed. The feed is going to be displayed on the startpage of http://linkedspending.aksw.org/.*/
public class NewsFeedWriter {

    /** external properties to be used in Project */
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");

    /**
     * Writes a xml-file into the newsfeed-folder. News get displayed on start screen of linkedspending.
     * @param title the title the news should have
     * @param description a more detailed description of the news
     * @throws IOException
     * @throws FeedException if the feed could not have been created
     */
    public static void writeNewsFeed(String title, String description) throws IOException, FeedException {
        SyndFeed feed = new SyndFeedImpl();

        feed.setFeedType("rss_2.0");

        feed.setTitle(title);

        feed.setLink(PROPERTIES.getProperty("urlNewsFeed"));

        feed.setDescription(description);

        Writer writer = new FileWriter(PROPERTIES.getProperty("pathNewsFeed")+title+".rss");

        SyndFeedOutput output = new SyndFeedOutput();

        output.output(feed,writer);

        writer.close();
    }

    public static void main(String[] args) throws IOException, FeedException

    {
        NewsFeedWriter.writeNewsFeed("SWP14 Newsfeed","This is a test for displaying News.");


    }
}
