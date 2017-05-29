package fitec.swag.first_updater;

import org.json.JSONArray;
import org.json.JSONObject;

public class Project {

    private JSONObject pBase;
    private JSONArray pComments;
    private String pDescription;
    private String pRisks;
    private int pFaqCount;
    private JSONArray pUpdates;

    public Project(JSONObject baseJson) {
        this.pBase = baseJson;
        this.pDescription = null;
        this.pComments = null;
        this.pRisks = null;
        this.pFaqCount = 0;
        this.pUpdates = null;
    }

    public Project(JSONObject base, String description, String risks, int faqCount,
                   JSONArray comments, JSONArray updates) {
        this.pBase = base;
        this.pDescription = description;
        this.pComments = comments;
        this.pRisks = risks;
        this.pFaqCount = faqCount;
        this.pUpdates = updates;
    }

    public int getId() {
        return pBase.getInt("id");
    }

    public String getUrl() {
        return pBase.getJSONObject("urls").getJSONObject("web").getString("project");
    }

    public String getCommentsUrl() {
        return this.getUrl() + "/comments";
    }

    public JSONObject getBaseJson() {
        return pBase;
    }

    public JSONArray getComments() {
        return pComments;
    }

    public void setComments(JSONArray commentsJson) {
        this.pComments = commentsJson;
    }

    public String getDescription() {
        return pDescription;
    }

    public void setDescription(String description) {
        this.pDescription = description;
    }

    public String getDescriptionUrl() {
        return this.getUrl() + "/description";
    }

    public String getRisks() {
        return pRisks;
    }

    public void setRisks(String risks) {
        this.pRisks = risks;
    }

    public int getFaqCount() {
        return pFaqCount;
    }

    public void setFaqCount(int faqCount) {
        this.pFaqCount = faqCount;
    }

    public JSONArray getUpdates() {
        return pUpdates;
    }

    public void setUpdates(JSONArray updates) {
        this.pUpdates = updates;
    }

    public String getUpdatesUrl() {
        return pBase.getJSONObject("urls").getJSONObject("web").getString("updates");
    }

    public boolean hasDescription() {
        return pDescription != null;
    }

    public boolean hasRisks() {
        return pRisks != null;
    }

    public boolean hasComments() {
        return pComments != null;
    }

    public boolean hasUpdates() {
        return pUpdates != null;
    }

    public org.bson.Document getBson() {
        JSONObject result = pBase;

        if (this.hasComments()) {
            result.put("comments", pComments);
        }

        if (this.hasUpdates()) {
            result.put("updates", pUpdates);
        }

        if (this.hasDescription()) {
            result.put("description", pDescription);
        }
        if (this.hasRisks()) {
            result.put("risks", pRisks);
        }
        result.put("faqCount", pFaqCount);
        return org.bson.Document.parse(result.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Project project = (Project) o;
        return this.getId() == project.getId();
    }

    @Override
    public int hashCode() {
        return this.getId();
    }
}