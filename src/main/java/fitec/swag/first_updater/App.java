package fitec.swag.first_updater;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.DataNode;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class App {
	private static Logger logger = LogManager.getLogger(App.class);

	private String mongoURI = "mongodb://localhost:27017";
	private MongoClient mongoClient;
	private MongoDatabase mongoDatabase;
	@SuppressWarnings("rawtypes")
	private MongoCollection outputCollection;
	private String databaseName = "crowdfunding";
	private String collectionName = "project";

	private String urlBase = "https://www.kickstarter.com/discover/advanced?sort=newest&page=";

	private ArrayList<org.bson.Document> mongoDocuments = new ArrayList<>();
	private JSONArray mongoDocumentsJsonArray = new JSONArray();

	private HashSet<Integer> idsProjetsCrawles = new HashSet<>();

	private List<Exception> exceptions = new ArrayList<>();

	private int cptProjects = 0;

	private PropertiesConfiguration config;

	public App() throws ConfigurationException {
		super();
		FileBasedConfigurationBuilder<PropertiesConfiguration> builder = new FileBasedConfigurationBuilder<PropertiesConfiguration>(
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
		getIdsProjects();
		try {
			doUpdate();
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
			exceptions.add(e);
		}
		logger.info("coucou");
		System.out.println("coucou");
		if (exceptions.isEmpty()) {
			if (!mongoDocuments.isEmpty()) {
				mongoClient = new MongoClient(new MongoClientURI(this.mongoURI));
				mongoClient.getDatabase(databaseName).getCollection("test").insertMany(mongoDocuments);
			} else {
				logger.info("pas de nouveau projets");
			}
		} else {
			logger.info("erreur-s lors de l'upadte, documents non pushés dans mongo");
		}
		try {
			FileUtils.writeStringToFile(new File("update-" + getFormattedDate() + ".json"),
					mongoDocumentsJsonArray.toString(), StandardCharsets.UTF_8);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			e.printStackTrace();
		}
		mongoClient.close();
	}

	private void initFromProperties() {
		this.mongoURI = config.getString("mongo.uri");
		this.databaseName = config.getString("mongo.database");
		this.collectionName = config.getString("mongo.collection");
	}

	private void initMongo() {
		mongoClient = new MongoClient(new MongoClientURI(this.mongoURI));
		mongoDatabase = mongoClient.getDatabase(databaseName);
		outputCollection = mongoDatabase.getCollection("test");
	}

	private void getIdsProjects() {
		// Construction de la requête
		// BasicDBObject allQuery = new BasicDBObject();
		BasicDBObject fields = new BasicDBObject();
		fields.put("id", 1);
		fields.put("_id", 0);

		// on ajoute les ids à un hashset
		System.out.println("getting projects ids from mongo database");
		mongoClient = new MongoClient(new MongoClientURI(this.mongoURI));
		try (MongoCursor<org.bson.Document> cursor = mongoClient.getDatabase(databaseName).getCollection(collectionName)
				.find().projection(fields).iterator()) {
			while (cursor.hasNext()) {
				org.bson.Document document = cursor.next();
				idsProjetsCrawles.add(document.getInteger("id"));
			}
		}
		mongoClient.close();

		logger.info(idsProjetsCrawles.size() + " projets en base");
	}

	private void doUpdate() throws IOException {
		Elements scriptTags;
		int nbPage = 1;
		boolean running = true;
		do {
			String url = this.urlBase + nbPage;
			System.out.println("scraping page : " + url);
			Document doc;
			doc = Jsoup.connect(url).get();
			// scriptTags =
			// doc.getElementsByClass("project-thumbnail-wrap");
			scriptTags = doc.getElementsByAttribute("data-project_pid");

			if (scriptTags != null && !scriptTags.isEmpty()) {
				// récupération du JSON contenant les infos de chaque projet
				// Collection<JSONObject> collection = new ArrayList<>();

				for (int i = 0; i < scriptTags.size(); i++) {
					Element element = scriptTags.get(i);
					String urlProjet = "https://www.kickstarter.com"
							+ element.getElementsByAttribute("href").attr("href");
					int id = Integer.parseInt(element.attr("data-project_pid"));

					if (!idsProjetsCrawles.contains(id)) {
						JSONObject jsonObject = buildJSONObject(urlProjet);
						org.bson.Document document = org.bson.Document.parse(jsonObject.toString());
						// document.put("_id", jsonObject.getInt("id"));
						mongoDocuments.add(document);
						mongoDocumentsJsonArray.put(jsonObject);
					} else {
						logger.info("skipping project " + urlProjet + " - " + id);
						running = false;
					}
				}
			} else {
				running = false;
			}
			nbPage++;
		} while (running && cptProjects <= 20);
	}

	private JSONObject buildJSONObject(String url) throws IOException {
		Document doc;
		JSONObject jsonObject = null;
		System.out.println("scraping project " + (++cptProjects) + " : " + url);
		doc = Jsoup.connect(url).get();
		Elements scriptTags = doc.getElementsByTag("script");
		for (Element tag : scriptTags) {
			for (DataNode node : tag.dataNodes()) {
				BufferedReader reader = new BufferedReader(new StringReader(node.getWholeData()));
				String line = null;
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

	private String getFormattedDate() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat format1 = new SimpleDateFormat("yyyy_MM_dd-kk_mm");
		return format1.format(cal.getTime());
	}
}
