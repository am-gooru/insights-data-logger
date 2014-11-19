package org.ednovo.data.model;

import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity(name = "resource")
public class ResourceCo {

	@Id
	private String id;
	@Column
	private String gooruOId;
	@Column
	private Long contentId;
	@Column
	private String title;
	@Column
	private String url;
	@Column
	private String description;
	@Column
	private String mediaType;
	@Column
	private String recordSource;
	@Column
	private Boolean isOer;
	@Column
	private String depthOfknowledge;
	@Column
	private String momentsOfLearning;
	@Column
	private String sharing;
	@Column
	private Date createdOn;
	@Column
	private String folder;
	@Column
	private String thumbnail;
	@Column
	private String resourceType;
	@Column
	private String typeEscaped;
	@Column
	private String category;
	@Column
	private String depthOfKnowledge;
	@Column
	private String educationalUse;
	@Column
	private String momentsofLearning;
	@Column
	private UserCo creator;
	@Column
	private UserCo owner;
	@Column
	private String attribution;
	@Column
	private String assetURI;
	@Column
	private String grade;
	@Column
	private Integer frameBreaker;
	@Column
	private StatisticsCo statistics;
	@Column
	private Date addDate;
	@Column	
	private Date lastModified;
	@Column
	private String versionUid;
	@Column
	private LicenseCo license;
	@Column
	private String resourceFormat;
	@Column
	private String instructional;
	@Column
	private String distinguish;
	@Column
	private String sourceType;
	@Column
	private CollectionCo sCollectionCo;
	@Column
	private QuestionCo question;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getGooruOId() {
		return gooruOId;
	}
	public void setGooruOId(String gooruOId) {
		this.gooruOId = gooruOId;
	}
	public Long getContentId() {
		return contentId;
	}
	public void setContentId(Long contentId) {
		this.contentId = contentId;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getMediaType() {
		return mediaType;
	}
	public void setMediaType(String mediaType) {
		this.mediaType = mediaType;
	}
	public String getRecordSource() {
		return recordSource;
	}
	public void setRecordSource(String recordSource) {
		this.recordSource = recordSource;
	}
	public Boolean getIsOer() {
		return isOer;
	}
	public void setIsOer(Boolean isOer) {
		this.isOer = isOer;
	}
	public String getDepthOfknowledge() {
		return depthOfknowledge;
	}
	public void setDepthOfknowledge(String depthOfknowledge) {
		this.depthOfknowledge = depthOfknowledge;
	}
	public String getMomentsOfLearning() {
		return momentsOfLearning;
	}
	public void setMomentsOfLearning(String momentsOfLearning) {
		this.momentsOfLearning = momentsOfLearning;
	}
	public String getSharing() {
		return sharing;
	}
	public void setSharing(String sharing) {
		this.sharing = sharing;
	}
	public Date getCreatedOn() {
		return createdOn;
	}
	public void setCreatedOn(Date createdOn) {
		this.createdOn = createdOn;
	}
	public String getFolder() {
		return folder;
	}
	public void setFolder(String folder) {
		this.folder = folder;
	}
	public String getThumbnail() {
		return thumbnail;
	}
	public void setThumbnail(String thumbnail) {
		this.thumbnail = thumbnail;
	}
	public String getResourceType() {
		return resourceType;
	}
	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}
	public String getTypeEscaped() {
		return typeEscaped;
	}
	public void setTypeEscaped(String typeEscaped) {
		this.typeEscaped = typeEscaped;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getDepthOfKnowledge() {
		return depthOfKnowledge;
	}
	public void setDepthOfKnowledge(String depthOfKnowledge) {
		this.depthOfKnowledge = depthOfKnowledge;
	}
	public String getEducationalUse() {
		return educationalUse;
	}
	public void setEducationalUse(String educationalUse) {
		this.educationalUse = educationalUse;
	}
	public String getMomentsofLearning() {
		return momentsofLearning;
	}
	public void setMomentsofLearning(String momentsofLearning) {
		this.momentsofLearning = momentsofLearning;
	}
	public UserCo getCreator() {
		return creator;
	}
	public void setCreator(UserCo creator) {
		this.creator = creator;
	}
	public UserCo getOwner() {
		return owner;
	}
	public void setOwner(UserCo owner) {
		this.owner = owner;
	}
	public String getAttribution() {
		return attribution;
	}
	public void setAttribution(String attribution) {
		this.attribution = attribution;
	}
	public String getAssetURI() {
		return assetURI;
	}
	public void setAssetURI(String assetURI) {
		this.assetURI = assetURI;
	}
	public void setGrade(String grade) {
		this.grade = grade;
	}
	public String getGrade() {
		return grade;
	}
	public void setStatistics(StatisticsCo statistics) {
		this.statistics = statistics;
	}
	public StatisticsCo getStatistics() {
		return statistics;
	}
	public void setAddDate(Date addDate) {
		this.addDate = addDate;
	}
	public Date getAddDate() {
		return addDate;
	}
	public void setLastModified(Date lastModified) {
		this.lastModified = lastModified;
	}
	public Date getLastModified() {
		return lastModified;
	}
	public void setVersionUid(String versionUid) {
		this.versionUid = versionUid;
	}
	public String getVersionUid() {
		return versionUid;
	}
	public void setLicense(LicenseCo license) {
		this.license = license;
	}
	public LicenseCo getLicense() {
		return license;
	}
	public Integer getFrameBreaker() {
		return frameBreaker;
	}
	public void setFrameBreaker(Integer frameBreaker) {
		this.frameBreaker = frameBreaker;
	}
	public void setResourceFormat(String resourceFormat) {
		this.resourceFormat = resourceFormat;
	}
	public String getResourceFormat() {
		return resourceFormat;
	}
	public void setInstructional(String instructional) {
		this.instructional = instructional;
	}
	public String getInstructional() {
		return instructional;
	}
	public void setDistinguish(String distinguish) {
		this.distinguish = distinguish;
	}
	public String getDistinguish() {
		return distinguish;
	}
	public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}
	public String getSourceType() {
		return sourceType;
	}
	public void setsCollectionCo(CollectionCo sCollectionCo) {
		this.sCollectionCo = sCollectionCo;
	}
	public CollectionCo getsCollectionCo() {
		return sCollectionCo;
	}
	public void setQuestion(QuestionCo question) {
		this.question = question;
	}
	public QuestionCo getQuestion() {
		return question;
	}
}
