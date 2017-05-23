package fitec.swag.first_updater;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.DataNode;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class App {
    private static final int maxNProjects = 4000;
    private static Logger logger = LogManager.getLogger(App.class);
    private MongoClient mongoClient;
    private String mongoURI = "Fitec@mongodb://localhost:27017";
    private String databaseName = "crowdfunding";
    private String collectionName = "kickstarter";

    private String urlBase = "https://www.kickstarter.com/discover/advanced?sort=newest&page=";

    private HashSet<Integer> idsProjetsCrawles = new HashSet<>();
    private List<Exception> exceptions = new ArrayList<>();

    private int cptProjects;

    private PropertiesConfiguration config;

    private App() throws ConfigurationException {
        super();
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder = new FileBasedConfigurationBuilder<>(
                PropertiesConfiguration.class).configure(
                new Parameters().properties().setFileName("config.properties").setThrowExceptionOnMissing(true)
                        .setListDelimiterHandler(new DefaultListDelimiterHandler(';'))
                        .setIncludesAllowed(false));
        this.config = builder.getConfiguration();
    }

    public static void main(String[] args) throws ConfigurationException {
        App app = new App();
        app.run();
    }

    private void run() {
        initFromProperties();
        mongoClient = new MongoClient(new MongoClientURI(this.mongoURI));
        getIdsProjects();
        try {
            doUpdate();
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
            exceptions.add(e);
        }
        mongoClient.close();
        if (exceptions.isEmpty()) {
            logger.info("update terminée sans erreur");
        } else {
            logger.info("erreur(s) lors de l'update");
        }
    }

    private void getIdsProjects() {
        BasicDBObject fields = new BasicDBObject(); // construction de la requête
        fields.put("id", 1);
        fields.put("_id", 0);
        System.out.println("Getting project ids from MongoDB");
        try (MongoCursor<org.bson.Document> cursor = mongoClient.getDatabase(databaseName).getCollection(collectionName)
                .find().projection(fields).iterator()) {
            while (cursor.hasNext()) {
                org.bson.Document document = cursor.next();
                idsProjetsCrawles.add(document.getInteger("id"));
            }
        }
        logger.info(idsProjetsCrawles.size() + " projects in the database");
    }

    private void doUpdate() throws IOException {
        Elements scriptTags;
        int nbPage = 1; // start from page 1

        boolean running = true;
        while (running && cptProjects < maxNProjects) {
            String url = this.urlBase + nbPage;
            System.out.println("scraping page : " + url);
            org.jsoup.nodes.Document doc;
            try {
                doc = Jsoup.connect(url).get();
            } catch (Exception e) {
                break;
            }
            scriptTags = doc.getElementsByAttribute("data-project_pid");
            if (scriptTags != null && !scriptTags.isEmpty()) {
                // récupération du JSON contenant les infos de chaque projet
                for (Element element : scriptTags) {
                    String urlProjet = "https://www.kickstarter.com"
                            + element.getElementsByAttribute("href").attr("href");
                    int id = Integer.parseInt(element.attr("data-project_pid"));
                    JSONObject jsonObject = buildJSONObject(urlProjet);
                    org.bson.Document document = org.bson.Document.parse(jsonObject.toString());

                    if (!idsProjetsCrawles.contains(id)) {
                        logger.info("inserting project " + urlProjet + " - " + id);
                        mongoClient.getDatabase(databaseName).getCollection(collectionName)
                                .insertOne(document);
                    } else {
                        logger.info("updating project " + urlProjet + " - " + id);
                        Bson filter = new org.bson.Document("id", document.getInteger("id"));
                        Bson update = new org.bson.Document("$set", document);
                        UpdateOptions options = new UpdateOptions().upsert(true);
                        mongoClient.getDatabase(databaseName).getCollection(collectionName)
                                .updateOne(filter, update, options);
                    }
                    idsProjetsCrawles.remove(id);
                }

                // update les projets déjà dans la base mais plus dans la liste sur le site
                for (int id : idsProjetsCrawles) {
                    BasicDBObject whereQuery = new BasicDBObject();
                    whereQuery.put("id", id);

                    BasicDBObject fields = new BasicDBObject();
                    fields.put("\"urls.web.project\":1", 1);
                    fields.put("_id", 0);

                    org.bson.Document myDoc = mongoClient.getDatabase(databaseName).getCollection(collectionName)
                            .find(whereQuery).projection(fields).first();

                    String urlProjet = myDoc.getString("urls.web.project");
                    JSONObject jsonObject = buildJSONObject(urlProjet);
                    org.bson.Document document = org.bson.Document.parse(jsonObject.toString());

                    logger.info("updating project " + urlProjet + " - " + id);
                    Bson filter = new org.bson.Document("id", document.getInteger("id"));
                    Bson update = new org.bson.Document("$set", document);
                    UpdateOptions options = new UpdateOptions().upsert(true);
                    mongoClient.getDatabase(databaseName).getCollection(collectionName)
                            .updateOne(filter, update, options);
                }
            } else {
                running = false;
            }
            nbPage++;
        }
    }

    private JSONObject buildJSONObject(String url) throws IOException {
        org.jsoup.nodes.Document doc;
        JSONObject jsonObject = null;
        System.out.println("scraping project " + (++cptProjects) + " : " + url);
        doc = Jsoup.connect(url).get();
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

    private void initFromProperties() {
        // setup MongoDB connection
        this.mongoURI = config.getString("mongo.uri");
        this.databaseName = config.getString("mongo.database");
        this.collectionName = config.getString("mongo.collection");
    }

}
