package fitec.swag.first_updater;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.DataNode;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class App {
    /**
     * Logger object
     */
    private static final Logger logger = LogManager.getLogger(App.class);

    /**
     * Maximum number of projects to scrap
     */
    private static final int DEFAULT_MAX_N_PROJECTS = 10000;

    /**
     * Default MongoDB URI
     */
    private static final String DEFAULT_MONGO_URI = "Fitec@mongodb://localhost:27017";

    /**
     * Default DB name
     */
    private static final String DEFAULT_DATABASE_NAME = "crowdfunding";

    /**
     * Default collection name
     */
    private static final String DEFAULT_COLLECTION_NAME = "kickstarter";

    /**
     * Default URL with project listings (without page number)
     */
    private static final String DEFAULT_URL_BASE = "https://www.kickstarter.com/discover/advanced?sort=newest&page=";

    /**
     * MongoDB URI, DB name and collection name (to load from config)
     */
    private String mongoURI;
    private String databaseName;
    private String collectionName;

    private App() throws ConfigurationException {
        try {
            FileBasedConfigurationBuilder<PropertiesConfiguration> builder = new FileBasedConfigurationBuilder<>(
                    PropertiesConfiguration.class).configure(
                    new Parameters().properties().setFileName("config.properties").setThrowExceptionOnMissing(true)
                            .setListDelimiterHandler(new DefaultListDelimiterHandler(';'))
                            .setIncludesAllowed(false));
            PropertiesConfiguration config = builder.getConfiguration();
            this.mongoURI = config.getString("mongo.uri");
            this.databaseName = config.getString("mongo.database");
            this.collectionName = config.getString("mongo.collection");
        } catch (ConfigurationException e) {
            System.out.println("Error importing configuration, using default values");
            this.mongoURI = DEFAULT_MONGO_URI;
            this.databaseName = DEFAULT_DATABASE_NAME;
            this.collectionName = DEFAULT_COLLECTION_NAME;
        }
    }

    public static void main(String[] args) throws ConfigurationException {
        App app = new App();
        app.run();
    }

    private void run() {
        List<Exception> exceptions = new ArrayList<>();
        MongoCollection collection;
        HashSet<Integer> ids;
        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(this.mongoURI))) {
            collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
            ids = getProjectIds(collection);
            doUpdate(ids, collection, DEFAULT_URL_BASE);
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
            exceptions.add(e);
        } finally {
            if (exceptions.isEmpty()) {
                logger.info("Update completed without errors");
            } else {
                System.out.print("Something went wrong :(");
                logger.info("Errors during update, imported data may be incomplete");
            }
        }
    }

    /**
     * Main updater method
     */
    private void doUpdate(HashSet<Integer> ids, MongoCollection collection, String urlBase) throws IOException {
        // page counter
        int nbPage = 1;
        // scraped projects counter
        int cptProjects = 0;
        boolean running = true;

        while (running && cptProjects < DEFAULT_MAX_N_PROJECTS) {
            String url = urlBase + nbPage;
            System.out.println("scraping page : " + url);
            Document doc;
            try {
                // retrieve one page with projects
                doc = getJsoupFromUrl(url);
            } catch (IOException e) {
                logger.info("Error retrieving page " + nbPage + " at " + url);
                break;
            }
            Elements scriptTags = doc.getElementsByAttribute("data-project_pid");
            if (scriptTags != null && !scriptTags.isEmpty()) {
                // get JSON for each project listed
                for (Element element : scriptTags) {
                    int id = Integer.parseInt(element.attr("data-project_pid"));
                    String urlProjet = "https://www.kickstarter.com"
                            + element.getElementsByAttribute("href").attr("href");
                    System.out.println("scraping project " + (++cptProjects) + " : " + urlProjet);
                    Project project;
                    try {
                        project = buildProjectFromUrl(urlProjet);
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.out.print("Error scraping project " + urlProjet);
                        System.out.print("Skipping...");
                        continue;
                    }
                    if (!ids.contains(id)) {
                        addToCollection(collection, project);
                    } else {
                        updateCollection(collection, project);
                    }
                    ids.remove(id);
                }
            } else {
                running = false;
            }
            nbPage++;
        }

        // update les projets déjà dans la base mais plus sur le site
        for (int id : ids) {
            // get project data of a project from the database
            org.bson.Document myDoc = getUrlAndStateDataFromDB(collection, id);
            // skip if dead
            if (myDoc.getString("state").equals("live")) {
                String urlProjet = myDoc.getString("urls.web.project");
                Project project;
                try {
                    project = buildProjectFromUrl(urlProjet);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.print("Error scraping project " + urlProjet);
                    System.out.print("Skipping...");
                    continue;
                }
                updateCollection(collection, project);
            }
        }
    }

    /**
     * Build a {@link Project} object from URL
     */
    private Project buildProjectFromUrl(String url) throws IOException {
        // get base json
        JSONObject baseJson;
        try {
            baseJson = buildBaseJSONFromURL(url);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        // create new project object from base json
        Project project = new Project(baseJson);

        // add description, risks and FAQ
        String descriptionUrl = project.getDescriptionUrl();
        Document doc_descr = getJsoupFromUrl(descriptionUrl);
        String description = getDescriptionFromJsoup(doc_descr);
        String risks = getRisksFromJsoup(doc_descr);
        int faqCount = getFaqFromJsoup(doc_descr);

        project.setDescription(description);
        project.setRisks(risks);
        project.setFaqCount(faqCount);

        // add updates
        String updatesUrl = project.getUpdatesUrl();
        JSONArray updates = getUpdatesFromJsoup(updatesUrl);
        project.setUpdates(updates);

        // add comments
        JSONArray comments = getCommentsForProject();
        project.setComments(comments);

        return project;
    }

    /**
     * Wrapper method to construct a document for insertion
     */
    private JSONObject buildBaseJSONFromURL(String url) throws IOException {
        Document jsoupDoc = getJsoupFromUrl(url);
        return buildBaseJSONObject(jsoupDoc);
    }

    /**
     * Convert a JSONObject to a BSON document
     */
    private org.bson.Document buildDocumentFromJSON(JSONObject jsonObject) {
        return org.bson.Document.parse(jsonObject.toString());
    }

    /**
     * Get projects IDs from the database
     */
    private HashSet<Integer> getProjectIds(MongoCollection collection) {
        HashSet<Integer> ids = new HashSet<>();
        BasicDBObject fields = new BasicDBObject();
        fields.put("id", 1);
        fields.put("_id", 0);
        System.out.println("Getting project ids from MongoDB...");
        MongoCursor<org.bson.Document> cursor = collection.find().projection(fields).iterator();
        try {
            while (cursor.hasNext()) {
                org.bson.Document document = cursor.next();
                ids.add(document.getInteger("id"));
            }
        } finally {
            cursor.close();
        }
        System.out.println(ids.size() + " projects in the DB");
        return ids;
    }

    /**
     * Retrieve Jsoup from a URL
     */
    private Document getJsoupFromUrl(String url) throws IOException {
        Document doc;
        try {
            doc = Jsoup.connect(url).get();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return doc;
    }

    /**
     * Construct base JSON object for a given project
     **/
    private JSONObject buildBaseJSONObject(Document doc) throws IOException {
        JSONObject jsonObject = null;
        Elements scriptTags = doc.getElementsByTag("script");
        for (Element tag : scriptTags) {
            for (DataNode node : tag.dataNodes()) {
                BufferedReader reader = new BufferedReader(new StringReader(node.getWholeData()));
                String line;
                do {
                    line = reader.readLine();
                    if (line != null && line.startsWith("  window.current_project")) {
                        String jsonEncoded = line.substring(28, line.length() - 2);
                        String jsonDecoded = StringEscapeUtils.unescapeHtml4(jsonEncoded).replaceAll("\\\\\\\\",
                                "\\\\");
                        jsonObject = new JSONObject(jsonDecoded);
                    }
                } while (line != null);
                reader.close();
            }
        }
        return jsonObject;
    }

    /**
     * Wrapper method to construct a document for insertion
     */
    private org.bson.Document buildDocumentFromURL(String url) throws IOException {
        Document jsoupDoc = getJsoupFromUrl(url);
        JSONObject jsonObject = buildBaseJSONObject(jsoupDoc);
        return buildDocumentFromJSON(jsonObject);
    }

    /**
     * Get URL and state for projects that are not listed
     */
    private org.bson.Document getUrlAndStateDataFromDB(MongoCollection coll, int id) {
        BasicDBObject whereQuery = new BasicDBObject();
        whereQuery.put("id", id);

        BasicDBObject fields = new BasicDBObject();
        fields.put("\"urls.web.project\":1", 1);
        fields.put("state", 1);
        fields.put("_id", 0);

        // get project data from project id from the database
        return (org.bson.Document) coll.find(whereQuery).projection(fields).first();
    }

    /**
     * Update a collection with a project
     */
    private void updateCollection(MongoCollection collection, Project project) {
        int id = project.getId();
        System.out.println("Updating project " + id + " (already in the DB)");
        Bson filter = new org.bson.Document("id", id);
        Bson update = new org.bson.Document("$set", project.getBson());
        collection.updateOne(filter, update);
    }

    /**
     * Insert new document into a collection
     */
    private void addToCollection(MongoCollection collection, Project project) {
        int id = project.getId();
        System.out.println("Inserting new project " + id + " into the DB");
        collection.insertOne(project.getBson());
    }

    /**
     * Get description for a project
     */
    private String getDescriptionFromJsoup(Document doc) throws IOException {
        logger.info("Scraping description...");
        String description = null;
        Elements scriptTags = doc.getElementsByClass("full-description");
        if (scriptTags != null && !scriptTags.isEmpty()) {
            description = scriptTags.text().replaceAll("\\s+", " ").trim();
        }
        return description;
    }

    /**
     * Get risks for a project
     */
    private String getRisksFromJsoup(Document doc) throws IOException {
        logger.info("Scraping risks...");
        String risks = null;
        Elements risksElems = doc.select(".js-risks");
        if (risksElems != null && !risksElems.isEmpty()) {
            risks = risksElems.text().replaceAll("\\s+", " ").trim();
        }
        return risks;
    }

    /**
     * Get FAQ count for a project
     */
    private int getFaqFromJsoup(Document doc) throws IOException {
        logger.info("Scraping FAQ count...");
        Elements faqElements = doc.select("[data-content=faqs] .count");
        int faqCount = (!faqElements.isEmpty()) ? Integer.parseInt(faqElements.text()) : 0;
        return faqCount;
    }

    /**
     * Get updates for a project
     */
    private JSONArray getUpdatesFromJsoup(String urlUpdates) throws IOException {
        JSONArray updates = new JSONArray();
        int nbPage = 1;
        logger.info("Scraping updates...");

        Elements updateElements;
        do {
            String url = urlUpdates + "?page=" + nbPage;
            Document doc;
            doc = getJsoupFromUrl(url);
            // lecture updates une par une
            updateElements = doc.select(".post");
            for (Element updateElement : updateElements) {
                String title = updateElement.select(".title").text()
                        .replaceAll("\\s+", " ").trim();
                String content = updateElement.select(".body").text()
                        .replaceAll("\\s+", " ").trim();
                JSONObject update = new JSONObject();
                update.put("title", title);
                update.put("content", content);
                updates.put(update);
            }
            nbPage++;
        } while (updateElements != null && !updateElements.isEmpty());
        return updates;
    }

    /**
     * Get comments for a project
     */
    private JSONArray getCommentsForProject() {
        logger.info("Scraping comments...");
        // TODO : finish implementing this method
        return new JSONArray();
    }
}