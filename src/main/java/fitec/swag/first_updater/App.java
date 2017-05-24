package fitec.swag.first_updater;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageConfidence;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
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
import com.mongodb.client.MongoCursor;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

public class App {
	private static Logger logger = LogManager.getLogger(App.class);

	private String mongoURI = "mongodb://localhost:27017";
	private MongoClient mongoClient;
	private String databaseName;
	private String collectionName;

	private String urlBase = "https://www.kickstarter.com/discover/advanced?sort=newest&page=";

	private ArrayList<org.bson.Document> mongoDocuments = new ArrayList<>();
	private JSONArray mongoDocumentsJsonArray = new JSONArray();

	private HashSet<Integer> idsProjetsCrawles = new HashSet<>();

	private List<Exception> exceptions = new ArrayList<>();

	private int cptProjects = 0;

	private PropertiesConfiguration config;

	LanguageDetector detector = new OptimaizeLangDetector();

	// properties de Stanford Core NLP
	StanfordCoreNLP stanfordSentiementPipeline;

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
		if (exceptions.isEmpty()) {
			logger.info("update terminée sans erreur");
			if (!mongoDocuments.isEmpty()) {
				mongoClient = new MongoClient(new MongoClientURI(this.mongoURI));
				mongoClient.getDatabase(databaseName).getCollection(collectionName).insertMany(mongoDocuments);
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
		// mongo
		this.mongoURI = config.getString("mongo.uri");
		this.databaseName = config.getString("mongo.database");
		this.collectionName = config.getString("mongo.collection");

		// stanford core nlp
		String[] stanfordNlpAnnotators = config.getStringArray("stanford.corenlp.annotators");
		Properties stanfordNlpProps = new Properties();
		stanfordNlpProps.setProperty("annotators", String.join(",", stanfordNlpAnnotators));
		stanfordSentiementPipeline = new StanfordCoreNLP(stanfordNlpProps);

		// optimaize language detector
		try {
			detector.loadModels();
		} catch (IOException e) {
			logger.error(e);
			e.printStackTrace();
		}
	}

	private void initMongo() {
		mongoClient = new MongoClient(new MongoClientURI(this.mongoURI));
	}

	private void getIdsProjects() {
		// Construction de la requête
		// BasicDBObject allQuery = new BasicDBObject();
		BasicDBObject fields = new BasicDBObject();
		fields.put("id", 1);
		fields.put("_id", 0);

		// on ajoute les ids à un hashset
		logger.info("getting projects ids from mongo database");
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
		outer: do {
			String url = this.urlBase + nbPage;
			logger.info("scraping page : " + url);
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
						logger.info("scraping project " + (++cptProjects) + " : " + url);
						Pair<JSONObject, Document> pair = buildJSONObject(urlProjet);
						JSONObject projectJson = pair.getLeft();
						Document projectJsoup = pair.getRight();
						org.bson.Document projectBson = org.bson.Document.parse(projectJson.toString());
						getDescriptionRisksAndFAQ(projectBson);
						getUpdates(projectBson, projectJsoup);
						getComments(projectBson, projectJsoup);
						// projectBson.put("_id", projectBson.getInteger("id"));
						mongoDocuments.add(projectBson);
						mongoDocumentsJsonArray.put(projectJson);
					} else {
						logger.info("skipping project " + urlProjet + " - " + id);
						running = false;
						break;
					}
				}
			} else {
				running = false;
			}
			nbPage++;
		} while (running);
	}

	private Pair<JSONObject, Document> buildJSONObject(String url) throws IOException {
		Document doc;
		JSONObject jsonObject = null;
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
		Pair<JSONObject, Document> pair = new ImmutablePair<JSONObject, Document>(jsonObject, doc);
		return pair;
	}

	private String getFormattedDate() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat format1 = new SimpleDateFormat("yyyy_MM_dd-kk_mm");
		return format1.format(cal.getTime());
	}

	private void getDescriptionRisksAndFAQ(org.bson.Document projectDocument) throws IOException {
		// update de la description
		String toReturn = null;
		Document doc;
		String url = projectDocument.get("urls", org.bson.Document.class).get("web", org.bson.Document.class)
				.getString("project") + "/description";
		logger.info("scraping descprition : " + projectDocument.getString("slug"));
		doc = Jsoup.connect(url).get();
		Elements scriptTags = doc.getElementsByClass("full-description");
		if (scriptTags != null && !scriptTags.isEmpty()) {
			toReturn = scriptTags.text().replaceAll("\\s+", " ").trim();
		}
		projectDocument.put("description", toReturn);

		// update des risques
		Elements risksElems = doc.select(".js-risks");
		if (risksElems != null && !risksElems.isEmpty()) {
			String risks = risksElems.text().replaceAll("\\s+", " ").trim();
			projectDocument.put("risks", risks);
		}

		// update des FAQ -- on ne récupère que leur nombre
		Elements faqElements = doc.select("[data-content=faqs] .count");
		int faqCounts = (!faqElements.isEmpty()) ? Integer.parseInt(faqElements.text()) : 0;
		projectDocument.put("faq_count", faqCounts);
	}

	private void getUpdates(org.bson.Document projectMongoDocument, Document projectJsoupDocument) throws IOException {
		// update du nombre d'updates
		int nbUpdates = Integer.parseInt(projectJsoupDocument.select("[data-content=updates] .count").text());

		// update des updates en tant que telles
		if (nbUpdates != 0) {
			projectMongoDocument.put("updates_count", nbUpdates);
			getUpdatesInner(projectMongoDocument);
		} else {
			logger.info("pas d'updates pour le projet : " + projectMongoDocument.getString("slug"));
		}
	}

	private void getUpdatesInner(org.bson.Document projectMongoDocument) throws IOException {
		int nbPage = 1;
		String urlUpdates = projectMongoDocument.get("urls", org.bson.Document.class)
				.get("web", org.bson.Document.class).getString("updates");
		org.bson.Document updates = new org.bson.Document();
		updates.put("data", new org.bson.BsonArray());
		logger.info("scraping updates  : " + projectMongoDocument.getString("slug"));

		Elements updateElements;
		do {
			String url = urlUpdates + "?page=" + nbPage;
			Document doc;
			doc = Jsoup.connect(url).get();
			// lecture updates une par une
			updateElements = doc.select(".post");
			for (Element updateElement : updateElements) {
				// logger.info("scraping update #" + cptUpdate++ + " of project
				// : " + projectJson.getString("slug"));
				String title = updateElement.select(".title").text().replaceAll("\\s+", " ").trim();
				String content = updateElement.select(".body").text().replaceAll("\\s+", " ").trim();

				org.bson.BsonDocument update = new org.bson.BsonDocument();
				update.append("data", new BsonString(String.join(" ", title, content)));
				updates.get("data", org.bson.BsonArray.class).add(update);
				// String metadata =
				// element.select("grid-post__metadata").text().replaceAll("\\s+",
				// " ").trim();
			}
			nbPage++;
		} while (updateElements != null && !updateElements.isEmpty());

		projectMongoDocument.put("updates", updates);
	}

	private void getComments(org.bson.Document projectMongoDocument, Document projectJsoupDocument) throws IOException {
		// mise à jour du nombre de documents
		int nbComments = updateCommentsCount(projectMongoDocument, projectJsoupDocument);

		// mise à jour des commentaires en tant que tels
		// if (nbComments != 0 && nbComments !=
		// projectMongoDocument.getInteger("comments_count")) {
		if (nbComments != 0) {
			projectMongoDocument.put("comments_count", nbComments);

			String urlComments = projectMongoDocument.get("urls", org.bson.Document.class)
					.get("web", org.bson.Document.class).getString("project") + "/comments";

			org.bson.Document comments = new org.bson.Document();
			comments.put("data", new BsonArray());
			logger.info("scraping comments  : " + projectMongoDocument.getString("slug"));

			boolean olderCommentToScrape = false;
			int cptPage = 0;

			// le vecteur des sentiments agrégés pour toutes les phrases de tous
			// les commentaires
			int[] allCommentsSentiments = { 0, 0, 0, 0, 0 };

			do {
				cptPage++;
				logger.debug("scraping comments page #" + cptPage);
				Document docComments;
				docComments = Jsoup.connect(urlComments).get();

				Elements commentsElements = docComments.select(".comment");
				for (Element commentElement : commentsElements) {
					String comment = commentElement.select("p").text().replaceAll("\\s+", " ").trim();
					if (comment.contains("This comment has been removed by Kickstarter.")) {
						continue;
					}
					org.bson.BsonDocument commentObject = new org.bson.BsonDocument();
					commentObject.put("data", new BsonString(comment));

					// on ajoute le vecteur de sentiments du commentaires
					// on détecte la langue du commentaire - on ne la prend en
					// compte que si la confiance est haute
					LanguageResult languageResult = detector.detect(comment);
					if (languageResult.getConfidence().equals(LanguageConfidence.HIGH)) {
						commentObject.put("lang",
								new BsonDocument("tika_optimaize", new BsonString(languageResult.getLanguage())));
						if ("en".equals(languageResult.getLanguage())) {
							populateSentimentForOneComment(comment, commentObject, allCommentsSentiments);
						}
					}

					populateBadgeForOneComment(commentElement, commentObject);
					comments.get("data", BsonArray.class).add(commentObject);
				}

				// récupération du lien "voir les commentaires plus anciens"
				Elements olderCommentsElements = docComments.select("a.older_comments");
				if (olderCommentsElements.size() > 0) {
					urlComments = "https://www.kickstarter.com"
							+ docComments.select("a.older_comments").get(0).attr("href");
					olderCommentToScrape = true;
				} else {
					olderCommentToScrape = false;
				}
			} while (olderCommentToScrape);

			populateSentimentsForAllComments(comments, allCommentsSentiments);

			projectMongoDocument.put("comments", comments);
		} else {
			logger.info("pas de commentaire pour le projet : " + projectMongoDocument.getString("slug"));
		}
	}

	private void populateBadgeForOneComment(Element commentElement, org.bson.BsonDocument commentObject) {
		// on regarde si la personne ayant commenté a un badge
		if (!commentElement.select(".repeat-creator-badge").isEmpty()) {
			commentObject.put("creator-badge", new BsonBoolean(true));
		}
		if (!commentElement.select(".superbacker-badge").isEmpty()) {
			commentObject.put("superbacker-badge", new BsonBoolean(true));
		}
	}

	private void populateSentimentsForAllComments(org.bson.Document comments, int[] allCommentsSentiments) {
		// ajout des sentiments pour tous les vecteurs
		int[] noSentiments = { 0, 0, 0, 0, 0 };
		if (!Objects.deepEquals(allCommentsSentiments, noSentiments)) {
			List<BsonInt32> sentimentsBson = new ArrayList<>();
			Arrays.stream(allCommentsSentiments).forEach(item -> sentimentsBson.add(new BsonInt32(item)));
			comments.put("sentiment",
					new BsonDocument("stanford", new BsonDocument("sentence-vector", new BsonArray(sentimentsBson))));
		}
	}

	private void populateSentimentForOneComment(String comment, org.bson.BsonDocument commentObject,
			int[] allCommentsSentiments) {
		Annotation annotation = stanfordSentiementPipeline.process(comment);
		List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);

		int[] sentiments = { 0, 0, 0, 0, 0 };
		for (CoreMap sentence : sentences) {
			String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
			switch (sentiment) {
			case "Very negative":
				sentiments[0]++;
				allCommentsSentiments[0]++;
				break;
			case "Negative":
				sentiments[1]++;
				allCommentsSentiments[1]++;
				break;
			case "Neutral":
				sentiments[2]++;
				allCommentsSentiments[2]++;
				break;
			case "Positive":
				sentiments[3]++;
				allCommentsSentiments[3]++;
				break;
			case "Very positive":
				sentiments[4]++;
				allCommentsSentiments[4]++;
				break;
			default:
				throw new IllegalStateException(sentiment
						+ " : sentiment should be either \"Very negative\", \"Negative\", \"Neutral\", \"Positive\", \"Very positive\"");
			}
		}
		List<BsonInt32> sentimentsBson = new ArrayList<>();
		Arrays.stream(sentiments).forEach(item -> sentimentsBson.add(new BsonInt32(item)));
		commentObject.put("sentiment",
				new BsonDocument("stanford", new BsonDocument("sentence-vector", new BsonArray(sentimentsBson))));
	}

	private int updateCommentsCount(org.bson.Document projectMongoDocument, Document projectJsoupDocument) {
		String nbCommentsString = projectJsoupDocument.select("[data-content=comments] .count").text();
		int nbComments;
		Pattern pattern = Pattern.compile("(\\d+.?\\d+)|\\d+");
		Matcher matcher = pattern.matcher(nbCommentsString);
		if (matcher.find()) {
			nbComments = Integer.parseInt(matcher.group().replaceAll("\\D", ""));
		} else {
			throw new RuntimeException(
					"nombre de commentaires introuvable pour le projet : " + projectMongoDocument.getString("slug"));
		}
		return nbComments;
	}
}
