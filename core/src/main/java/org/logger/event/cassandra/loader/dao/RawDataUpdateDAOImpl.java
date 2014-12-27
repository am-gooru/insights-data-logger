package org.logger.event.cassandra.loader.dao;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.JSONDeserializer;
import org.ednovo.data.model.LicenseCo;
import org.ednovo.data.model.PartyCustomFieldPo;
import org.ednovo.data.model.QuestionCo;
import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.SCollectionCo;
import org.ednovo.data.model.StatisticsCo;
import org.ednovo.data.model.TypeConverter;
import org.ednovo.data.model.UserCo;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.retry.ConstantBackoff;

import flexjson.JSONSerializer;

public class RawDataUpdateDAOImpl extends BaseDAOCassandraImpl implements RawDataUpdateDAO, Constants {

	private static final Logger logger = LoggerFactory.getLogger(MicroAggregatorDAOmpl.class);

	private CassandraConnectionProvider connectionProvider;

	private BaseCassandraRepoImpl baseCassandraDao;

    private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");

	public RawDataUpdateDAOImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		this.connectionProvider = connectionProvider;
		this.baseCassandraDao = new BaseCassandraRepoImpl(this.connectionProvider);
	}
	
	public ResourceCo updateResource(Map<String, Object> eventMap, ResourceCo resourceCo) {

		Map<String, Object> resourceMap = (Map<String, Object>) eventMap.get("resource");
		if (resourceMap != null) {
			if (resourceMap.containsKey("version")) {
				resourceCo.setVersion(Integer.valueOf(resourceMap.get("version").toString()));
			}
			resourceCo.setId(resourceMap.get("gooruOid").toString());
			resourceCo.setGooruOId(resourceMap.get("gooruOid").toString());
			resourceCo.setTitle(resourceMap.get("title").toString());
			resourceCo.setUrl(resourceMap.get("url").toString());
			resourceCo.setDescription(resourceMap.get("description").toString());
			resourceCo.setMediaType(resourceMap.get("mediaType") != null ? resourceMap.get("mediaType").toString() : null);
			resourceCo.setRecordSource("useradded");
			if (resourceMap.containsKey("isOer") && resourceMap.get("isOer") != null) {
				resourceCo.setIsOerBoolean((resourceMap.get("isOer") != null && ((Map<String, Integer>) eventMap.get("resource")).get("isOer").equals(1)) ? true : false);
				resourceCo.setIsOer((resourceMap.get("isOer") != null && ((Map<String, Integer>) eventMap.get("resource")).get("isOer").equals(1)) ? "1" : "0");
			}
			resourceCo.setSharing(resourceMap.get("sharing").toString());
			Long createdOnTimeStamp = Long.valueOf(resourceMap.get("createdOn").toString());
			Date createdOnDate = new Date(createdOnTimeStamp);
			resourceCo.setCreatedOn(createdOnDate);
			resourceCo.setFolder(resourceMap.get("folder").toString());
			resourceCo.setGrade(((resourceMap.containsKey("grade")) && (resourceMap.get("grade")) != null) ? resourceMap.get("grade").toString() : "");
			if ((resourceMap.containsKey("resourceFormat")) && (resourceMap.get("resourceFormat")) != null) {
				resourceCo.setResourceFormat(((Map<String, String>) resourceMap.get("resourceFormat")).get("value"));
			}
			if ((resourceMap.containsKey("instructional")) && (resourceMap.get("instructional")) != null) {
				resourceCo.setInstructional(((Map<String, String>) resourceMap.get("instructional")).get("value"));
			}

			String thumbnail = null;
			if (resourceMap.containsKey("thumbnails") && (resourceMap.get("thumbnails") != null)) {
				if (((Map<String, String>) resourceMap.get("thumbnails")).get("url") != null) {
					thumbnail = ((Map<String, String>) resourceMap.get("thumbnails")).get("url");
					thumbnail = thumbnail.substring(thumbnail.lastIndexOf("/") + 1);
				} else {
					thumbnail = "";
				}
			} else {
				thumbnail = "";
			}
			resourceCo.setThumbnail(thumbnail);

			String resourceType = null;
			if (resourceMap.containsKey(RESOURCETYPE) && (resourceMap.get(RESOURCETYPE) != null && ((Map<String, String>) resourceMap.get(RESOURCETYPE)).get(NAME) != null)) {
				resourceType = ((Map<String, String>) resourceMap.get(RESOURCETYPE)).get(NAME);
				String resourceTypeEscaped = null;
				if (resourceType.contains("-")) {
					resourceTypeEscaped = resourceType.replace("-", "");
				} else {
					resourceTypeEscaped = resourceType.replace("/", "");
				}
				resourceCo.setResourceType(resourceType);
				resourceCo.setTypeEscaped(resourceTypeEscaped);
			}
			if (resourceMap.containsKey("category") && resourceMap.get("category") != null) {
				resourceCo.setCategory(resourceMap.get("category").toString());
			}

			List<Map<String, Object>> depthOfKnowledgeList = ((Map<String, List<Map<String, Object>>>) eventMap.get("resource")).get("depthOfKnowledges");
			String depthOfKnowledgeString = new String();
			if (depthOfKnowledgeList != null && depthOfKnowledgeList.size() > 0) {
				for (Map<String, Object> depthOfKnowledge : depthOfKnowledgeList) {
					if (Boolean.valueOf(depthOfKnowledge.get("selected").toString())) {
						if (StringUtils.isEmpty(depthOfKnowledgeString)) {
							depthOfKnowledgeString = depthOfKnowledge.get("value").toString();
						} else {
							depthOfKnowledgeString += "," + depthOfKnowledge.get("value");
						}
					}
				}
				if (StringUtils.trimToNull(depthOfKnowledgeString) != null) {
					resourceCo.setDepthOfknowledge(depthOfKnowledgeString);
				}
			}
			if (((Map<String, List<Map<String, Object>>>) eventMap.get("resource")).containsKey("educationalUse")
					&& ((Map<String, List<Map<String, Object>>>) eventMap.get("resource")).get("educationalUse") != null) {
				List<Map<String, Object>> educationalUseList = ((Map<String, List<Map<String, Object>>>) eventMap.get("resource")).get("educationalUse");
				// Education in use for all resource
				if (educationalUseList != null && educationalUseList.size() > 0) {
					for (Map<String, Object> educationalInUse : educationalUseList) {
						if (Boolean.valueOf(educationalInUse.get("selected").toString())) {
							resourceCo.setEducationalUse(educationalInUse.get("value").toString());
						}
					}
				}
			}
			if (((Map<String, List<Map<String, Object>>>) eventMap.get("resource")).containsKey("momentsOfLearning")
					&& ((Map<String, List<Map<String, Object>>>) eventMap.get("resource")).get("momentsOfLearning") != null) {
				// Moments of learning for all resourceType except question
				List<Map<String, Object>> momentsOfLearningList = ((Map<String, List<Map<String, Object>>>) eventMap.get("resource")).get("momentsOfLearning");
				if (momentsOfLearningList != null && momentsOfLearningList.size() > 0) {
					for (Map<String, Object> momentsOfLearning : momentsOfLearningList) {
						if (Boolean.valueOf(momentsOfLearning.get("selected").toString())) {
							resourceCo.setMomentsOfLearning(momentsOfLearning.get("value").toString());
						}
					}
				}
			}
			if (resourceMap.get("creator") != null) {
				resourceCo.setCreator(this.getUser((Map<String, Object>) resourceMap.get("creator")));
			}
			if (resourceMap.get("user") != null) {
				resourceCo.setCreator(this.getUser((Map<String, Object>) resourceMap.get("user")));
			}
			if (resourceMap.containsKey("resourceSource") && resourceMap.get("resourceSource") != null) {
				Map<String, String> resourceSource = ((Map<String, Map<String, String>>) eventMap.get("resource")).get("resourceSource");
				resourceCo.setAttribution(resourceSource.get("attribution") != null ? resourceSource.get("attribution") : null);
				resourceCo.setSourceType(resourceSource.get("type") != null ? resourceSource.get("type") : null);
			}

			StatisticsCo statisticsCo = new StatisticsCo();
			Integer hasFrameBreakerN = 0;
			if (resourceMap.containsKey("hasFrameBreaker") && resourceMap.get("hasFrameBraker") != null) {
				hasFrameBreakerN = (Boolean.valueOf(resourceMap.get("hasFrameBraker").toString()) ? 1 : 0);
				resourceCo.setFrameBreaker(hasFrameBreakerN);
				statisticsCo.setHasFrameBreakerN(hasFrameBreakerN);
			}
			int invalidResource = 0;
			Integer hasNoThumbnail = 0;
			int hasNoDescription = 0;
			if (resourceMap.containsKey(RESOURCETYPE) && (resourceMap.get(RESOURCETYPE) != null && ((Map<String, String>) resourceMap.get(RESOURCETYPE)).get(NAME) != null)
					&& !((Map<String, String>) resourceMap.get(RESOURCETYPE)).get(NAME).equalsIgnoreCase("assessment-question")) {

				if (StringUtils.trimToNull(resourceMap.get("title").toString()) == null) {
					invalidResource = 1;
				}
				statisticsCo.setInvalidResource(invalidResource + "");

				if ((StringUtils.trimToNull(thumbnail) == null)
						&& !(StringUtils.isNotBlank(resourceMap.get("url").toString()) && StringUtils.contains(resourceMap.get("url").toString(), "://www.youtube.com"))) {
					hasNoThumbnail = 1;
				}

				if (resourceMap.get("description") != null && StringUtils.trimToNull(resourceMap.get("description").toString()) == null) {
					hasNoDescription = 1;
				}

				if ((resourceMap.get("collectionType") != null && resourceMap.get("collectionType").toString().equalsIgnoreCase("collection") && StringUtils.trimToNull(resourceMap.get("goals")
						.toString()) != null)) {
					hasNoDescription = 0;
				}
				statisticsCo.setHasNoThumbnail(hasNoThumbnail.toString());
				statisticsCo.setHasNoThumbnailN(hasNoThumbnail);
				statisticsCo.setHasNoDescription(hasNoDescription + "");

			}
			resourceCo.setStatistics(statisticsCo);
			resourceCo.setLastModified(new Date(Long.valueOf((Long) resourceMap.get("lastModified"))));
			resourceCo.setAddDate(createdOnDate);
			resourceCo.setVersionUid(UUID.randomUUID().toString());
			LicenseCo license = new LicenseCo();
			license.setName((resourceMap.get("license") != null && ((Map<String, String>) resourceMap.get("license")).get(NAME) != null) ? ((Map<String, String>) resourceMap.get("license")).get(NAME)
					.toString() : null);
			license.setCode((resourceMap.get("license") != null && ((Map<String, String>) resourceMap.get("license")).get("code") != null) ? ((Map<String, String>) resourceMap.get("license")).get(
					"code").toString() : null);
			license.setIcon((resourceMap.get("license") != null && ((Map<String, String>) resourceMap.get("license")).get("icon") != null) ? ((Map<String, String>) resourceMap.get("license")).get(
					"icon").toString() : null);
			license.setUrl((resourceMap.get("license") != null && ((Map<String, String>) resourceMap.get("license")).get("url") != null) ? ((Map<String, String>) resourceMap.get("license"))
					.get("url").toString() : null);
			resourceCo.setLicense(license);

			if (resourceType.equalsIgnoreCase("assessment-question")) {
				Map<String, Object> questionMap = new HashMap<String, Object>();
				questionMap = resourceMap;
				if (eventMap.containsKey("questionInfo") && eventMap.get("questionInfo") != null) {
					questionMap = (Map<String, Object>) eventMap.get("questionInfo");
				}
				QuestionCo questionCo = new QuestionCo();
				questionCo.setGc("0");
				questionCo.setQuestion((questionMap.containsKey("questionText") && questionMap.get("questionText") != null) ? questionMap.get("questionText").toString() : null);
				questionCo.setType((questionMap.containsKey("typeName") && questionMap.get("typeName") != null) ? questionMap.get("typeName").toString() : null);
				questionCo.setExplanation((questionMap.containsKey("explanation") && questionMap.get("explanation") != null) ? questionMap.get("explanation").toString() : null);
				if (questionMap.containsKey("answers") && questionMap.get("answers") != null) {
					List<Map<String, Object>> answers = (List<Map<String, Object>>) questionMap.get("answers");
					StringBuilder questionAnswerTexts = new StringBuilder();
					if (answers != null) {
						for (Map<String, Object> assessmentAnswer : answers) {
							if (questionAnswerTexts.length() > 0) {
								questionAnswerTexts.append(" ~~ ");
							}
							questionAnswerTexts.append(assessmentAnswer.get("answerText"));
						}
					}
					questionCo.setAnswerTexts(questionAnswerTexts.toString());
					questionCo.setAnswerOptionCount((answers != null) ? answers.size() : 0);
				}
				if (questionMap.containsKey("hints") && questionMap.get("hints") != null) {
					StringBuilder hintTexts = new StringBuilder();
					List<Map<String, Object>> hints = (List<Map<String, Object>>) questionMap.get("hints");
					if (hints != null) {
						for (Map<String, Object> assessmentHint : hints) {
							if (hintTexts.length() > 0) {
								hintTexts.append(" ~~ ");
							}
							hintTexts.append(assessmentHint.get("hintText"));
						}
					}
					questionCo.setHintCount((hints != null) ? hints.size() : 0);
					questionCo.setHintTexts(hintTexts.toString());
				}
				if (questionMap.containsKey("assets") && questionMap.get("assets") != null) {
					List<Map<String, Object>> assets = (List<Map<String, Object>>) questionMap.get("assets");
					for (Map<String, Object> assetAssoc : assets) {
						if (assetAssoc.get("assetKey") != null && assetAssoc.get("assetKey").toString().equalsIgnoreCase("asset-question")) {
							resourceCo.setThumbnail(((Map<String, String>) assetAssoc.get("asset")).get(NAME).toString());
							hasNoThumbnail = 0;
						} else if (assetAssoc.get("assetKey") != null && assetAssoc.get("assetKey").toString().equalsIgnoreCase("asset-explanation")) {
							questionCo.setExplanationAsset(((Map<String, String>) assetAssoc.get("asset")).get(NAME).toString());
						}
					}
				}
				resourceCo.setQuestion(questionCo);

				StatisticsCo statistics = resourceCo.getStatistics();
				if (statistics == null) {
					statistics = new StatisticsCo();
				}
				statistics.setHasNoThumbnail(hasNoThumbnail + "");
				statistics.setHasNoThumbnailN(hasNoThumbnail);
				resourceCo.setStatistics(statistics);

				if (StringUtils.isEmpty(((questionMap.containsKey("questionText") && questionMap.get("questionText") != null)) ? questionMap.get("questionText").toString() : "")) {
					invalidResource = 1;
				}
				statistics.setInvalidResource(invalidResource + "");

				if (StringUtils.isEmpty(((questionMap.containsKey("description") && questionMap.get("description") != null)) ? questionMap.get("description").toString() : "")) {
					hasNoDescription = 1;
				}
				statistics.setHasNoDescription(hasNoDescription + "");
			}
		}
		return resourceCo;

	}

	public ResourceCo updateCollection(Map<String, Object> eventMap, ResourceCo resourceCo) {

		SCollectionCo collectionCo = new SCollectionCo();
		resourceCo.setId(eventMap.get("gooruOid").toString());
		resourceCo.setGooruOId(eventMap.get("gooruOid").toString());

		resourceCo.setTitle(eventMap.get("title").toString());
		resourceCo.setVersion(Integer.valueOf(eventMap.get("version").toString()));
		resourceCo.setSharing(eventMap.get("sharing").toString());
		resourceCo.setLastModified(new Date(Long.valueOf(eventMap.get("lastModified").toString())));
		resourceCo.setCreatedOn(new Date(Long.valueOf(eventMap.get("createdOn").toString())));
		resourceCo.setAddDate(new Date(Long.valueOf(eventMap.get("createdOn").toString())));
		resourceCo.setFolder(eventMap.get("folder").toString());
		resourceCo.setLastUpdatedUserUid(eventMap.containsKey("lastUpdatedUserUid") ? eventMap.get("lastUpdatedUserUid").toString() : null);
		resourceCo.setMediaType((eventMap.containsKey("mediaType") && eventMap.get("mediaType") != null) ? eventMap.get("mediaType").toString() : null);
		if (eventMap.containsKey("assetURI") && eventMap.get("assetURI") != null) {
			resourceCo.setAssetURI(eventMap.get("assetURI").toString());
		}
		resourceCo.setGrade((eventMap.containsKey("grade") && eventMap.get("grade") != null) ? eventMap.get("grade").toString() : null);
		if ((eventMap.containsKey("resourceFormat")) && (eventMap.get("resourceFormat")) != null) {
			resourceCo.setResourceFormat(((Map<String, String>) eventMap.get("resourceFormat")).get("value"));
		}
		if ((eventMap.containsKey("instructional")) && (eventMap.get("instructional")) != null) {
			resourceCo.setInstructional(((Map<String, String>) eventMap.get("instructional")).get("value"));
		}

		String thumbnail = null;
		if (eventMap.containsKey("thumbnails") && eventMap.get("thumbnails") != null && ((Map<String, String>) eventMap.get("thumbnails")).get("url") != null) {
			thumbnail = ((Map<String, String>) eventMap.get("thumbnails")).get("url");
			thumbnail = thumbnail.substring(thumbnail.lastIndexOf("/") + 1);
		} else {
			thumbnail = "";
		}
		resourceCo.setThumbnail(thumbnail);

		String resourceType = null;
		if (eventMap.containsKey(RESOURCETYPE) && eventMap.get(RESOURCETYPE) != null && ((Map<String, String>) eventMap.get(RESOURCETYPE)).get(NAME) != null) {
			resourceType = ((Map<String, String>) eventMap.get(RESOURCETYPE)).get(NAME);
			String resourceTypeEscaped = null;
			if (resourceType.contains("-")) {
				resourceTypeEscaped = resourceType.replace("-", "");
			} else {
				resourceTypeEscaped = resourceType.replace("/", "");
			}
			resourceCo.setResourceType(resourceType);
			resourceCo.setTypeEscaped(resourceTypeEscaped);
		}
		if (eventMap.containsKey("creator") && eventMap.get("creator") != null) {
			resourceCo.setCreator(this.getUser((Map<String, Object>) eventMap.get("creator")));
		}
		if (eventMap.containsKey("user") && eventMap.get("user") != null) {
			resourceCo.setOwner(this.getUser((Map<String, Object>) eventMap.get("user")));
		}
		if (eventMap.containsKey("depthOfKnowledges") && eventMap.get("depthOfKnowledges") != null) {
			List<Map<String, Object>> depthOfKnowledgeList = ((List<Map<String, Object>>) eventMap.get("depthOfKnowledges"));
			String depthOfKnowledgeString = new String();
			if (depthOfKnowledgeList != null && depthOfKnowledgeList.size() > 0) {
				for (Map<String, Object> depthOfKnowledge : depthOfKnowledgeList) {
					if (Boolean.valueOf(depthOfKnowledge.get("selected").toString())) {
						if (StringUtils.isEmpty(depthOfKnowledgeString)) {
							depthOfKnowledgeString = depthOfKnowledge.get("value").toString();
						} else {
							depthOfKnowledgeString += "," + depthOfKnowledge.get("value");
						}
					}
				}
				if (StringUtils.trimToNull(depthOfKnowledgeString) != null) {
					resourceCo.setDepthOfknowledge(depthOfKnowledgeString);
				}
			}
		}
		
		resourceCo.setVersionUid(UUID.randomUUID().toString());
		resourceCo.setCopiedResourceId((eventMap.containsKey("copiedResourceId") && eventMap.get("copiedResourceId") != null) ? eventMap.get("copiedResourceId").toString() : null);
		collectionCo.setCollectionType((eventMap.containsKey("collectionType") && eventMap.get("collectionType") != null) ? eventMap.get("collectionType").toString() : null);
		collectionCo.setEstimatedTime((eventMap.containsKey("estimatedTime") && eventMap.get("estimatedTime") != null) ? eventMap.get("estimatedTime").toString() : null);
		collectionCo.setGoals((eventMap.containsKey("goals") && eventMap.get("goals") != null) ? eventMap.get("goals").toString() : null);
		collectionCo.setKeyPoints((eventMap.containsKey("keyPoints") && eventMap.get("keyPoints") != null) ? eventMap.get("keyPoints").toString() : null);
		collectionCo.setLanguage((eventMap.containsKey("language") && eventMap.get("language") != null) ? eventMap.get("language").toString() : null);
		collectionCo.setLanguageObjective((eventMap.containsKey("languageObjective") && eventMap.get("languageObjective") != null) ? eventMap.get("languageObjective").toString() : null);
		collectionCo.setNarrationLink((eventMap.containsKey("narrationLink") && eventMap.get("narrationLink") != null) ? eventMap.get("narrationLink").toString() : null);
		collectionCo.setNetwork((eventMap.containsKey("network") && eventMap.get("network") != null) ? eventMap.get("network").toString() : null);
		collectionCo.setNotes((eventMap.containsKey("notes") && eventMap.get("notes") != null) ? eventMap.get("notes").toString() : null);
		if (eventMap.containsKey("learningSkills") && eventMap.get("learningSkills") != null) {
			List<Map<String, Object>> learningSkillsList = ((List<Map<String, Object>>) eventMap.get("learningSkills"));
			if (learningSkillsList != null && learningSkillsList.size() > 0) {
				for (Map<String, Object> learningSkills : learningSkillsList) {
					if (Boolean.valueOf(learningSkills.get("selected").toString())) {
						collectionCo.setLearningAndInovation(learningSkills.get("value").toString());
					}
				}
			}
		}
		if (eventMap.containsKey("audience") && eventMap.get("audience") != null) {
			List<Map<String, Object>> audienceList = ((List<Map<String, Object>>) eventMap.get("audience"));
			if (audienceList != null && audienceList.size() > 0) {
				String audienceString = new String();
				if (audienceList != null && audienceList.size() > 0) {
					for (Map<String, Object> audience : audienceList) {
						if (Boolean.valueOf(audience.get("selected").toString())) {
							if (StringUtils.isEmpty(audienceString)) {
								audienceString = audience.get("value").toString();
							} else {
								audienceString += "," + audience.get("value");
							}
						}
					}
					if (StringUtils.trimToNull(audienceString) != null) {
						collectionCo.setAudience(audienceString);
					}
				}
			}
		}
		if (eventMap.containsKey("instructionalMethod") && eventMap.get("instructionalMethod") != null) {
			List<Map<String, Object>> instructionalMethodList = (List<Map<String, Object>>) eventMap.get("instructionalMethod");
			if (instructionalMethodList != null && instructionalMethodList.size() > 0) {
				for (Map<String, Object> instructionalMethod : instructionalMethodList) {
					if (Boolean.valueOf(instructionalMethod.get("selected").toString())) {
						collectionCo.setInstructionMethod(instructionalMethod.get("value").toString());
					}
				}
			}

		}
		resourceCo.setsCollectionCo(collectionCo);
		return resourceCo;
	}

	@SuppressWarnings("unchecked")
	public UserCo updateUser(Map<String, Object> eventMap, UserCo userCo) {
		
		userCo.setUserUid(eventMap.get("gooruUId").toString());
		userCo.setGooruUid(eventMap.get("gooruUId").toString());
		userCo.setFirstname((eventMap.containsKey("firstName") && eventMap.get("firstName") != null) ? eventMap.get("firstName").toString() : null);
		userCo.setLastname((eventMap.containsKey("lastName") && eventMap.get("lastName") != null) ? eventMap.get("lastName").toString() : null);
		userCo.setUsername((eventMap.containsKey("username") && eventMap.get("username") != null) ? eventMap.get("username").toString() : null);
		userCo.setDisplayname((eventMap.containsKey("usernameDisplay") && eventMap.get("usernameDisplay") != null) ? eventMap.get("usernameDisplay").toString() : null);
		userCo.setEmailId((eventMap.containsKey("emailId") && eventMap.get("emailId") != null) ? eventMap.get("emailId").toString() : null);
		userCo.setAccountRegisterType((eventMap.containsKey("accountCreatedType") && eventMap.get("accountCreatedType") != null) ? eventMap.get("accountCreatedType").toString() : null);
		userCo.setActive((eventMap.containsKey("active") && eventMap.get("active") != null) ? Short.valueOf(eventMap.get("active").toString()) : null);
		userCo.setConfirmStatus((eventMap.containsKey("confirmStatus") && eventMap.get("confirmStatus") != null) ? eventMap.get("confirmStatus").toString() : null);
		userCo.setDeleted(false);
		userCo.setRoleSet((eventMap.containsKey("userRoleSetString") && eventMap.get("userRoleSetString") != null) ? eventMap.get("userRoleSetString").toString() : null);
		userCo.setUserProfileImage((eventMap.containsKey("profileImageUrl") && eventMap.get("profileImageUrl") != null) ? eventMap.get("profileImageUrl").toString() : null);
		userCo.setVersionUid(UUID.randomUUID().toString());
		try {
			userCo.setCreatedOn((eventMap.containsKey("createdOn") && eventMap.get("createdOn") != null) ? dateFormatter.parse(eventMap.get("createdOn").toString()) : null);
			userCo.setLastModifiedOn((eventMap.containsKey("lastModifiedOn") && eventMap.get("lastModifiedOn") != null) ? dateFormatter.parse(eventMap.get("lastModifiedOn").toString()) : null);
			userCo.setLastLogin((eventMap.containsKey("lastLogin") && eventMap.get("lastLogin") != null) ? dateFormatter.parse(eventMap.get("lastLogin").toString()) : null);
		} catch (ParseException e) {
			logger.info("Unable to parse date fields");
		}
        String profileVisibility = "false";
		if (eventMap.containsKey("customFields") && eventMap.get("customFields") != null) {
			List<Map<String, Object>> customFields = (List<Map<String, Object>>) eventMap.get("customFields");
			if (customFields != null && customFields.size() > 0) {
				if (eventMap.containsKey("optionalKey") && eventMap.get("optionalKey").toString().equalsIgnoreCase("show_profile_page") && (eventMap.containsKey("optionalValue") && eventMap.get("optionalValue") != null)) {
					profileVisibility = (eventMap.get("optionalValue").toString());
				}
			}
		}
		userCo.setProfileVisibility(profileVisibility);
		if ((eventMap.containsKey("meta") && eventMap.get("meta") != null) || (eventMap.containsKey("optionalKey") && eventMap.get("optionalKey") != null && eventMap.get("optionalKey").toString().equalsIgnoreCase("user_taxonomy_root_code"))) {
			Map<String, Object> metaData = new HashMap<String, Object>();
			JSONSerializer metaDataSerializer = new JSONSerializer();
			List<String> taxonomyCodeIdList;
			List<String> taxonomyCodeList;
			if (eventMap.containsKey("optionalKey") && eventMap.get("optionalKey") != null && eventMap.get("optionalKey").toString().equalsIgnoreCase("user_taxonomy_root_code")) {
				if (eventMap.containsKey("optionalValue") && eventMap.get("optionalValue") != null) {
					taxonomyCodeIdList = (Arrays.asList(eventMap.get("optionalValue").toString().split(",")));
					if (taxonomyCodeIdList.size() > 0) {
						metaData.put("codeId", taxonomyCodeIdList);
					}
				}
			} else if (eventMap.containsKey("meta") && eventMap.get("meta") != null) {
				Map<String, Object> metaMap = (Map<String, Object>) eventMap.get("meta");
				Map<String, Object> taxonomyPreference = (Map<String, Object>) metaMap.get("taxonomyPreference");
				if (taxonomyPreference != null) {
					
					if (taxonomyPreference.containsKey("codeId") && taxonomyPreference.get("codeId") != null) {
						taxonomyCodeIdList = (List<String>) taxonomyPreference.get("codeId");
						if (taxonomyCodeIdList.size() > 0) {
							metaData.put("codeId", taxonomyCodeIdList);
						}
					}
					if (taxonomyPreference.containsKey("code") && taxonomyPreference.get("code") != null) {
						taxonomyCodeList = (List<String>) taxonomyPreference.get("code");
						if (taxonomyCodeList.size() > 0) {
							metaData.put("code", taxonomyCodeList);
						}
					}
					
				}
			}
			if (!metaData.isEmpty()) {
				userCo.setMetaJson(metaDataSerializer.deepSerialize(metaData));
			}
		}
		
		

		if((eventMap.containsKey("parentUser") && eventMap.get("parentUser") != null) && (eventMap.containsKey("accountCreatedType") && eventMap.get("accountCreatedType") != null && eventMap.get("accountCreatedType").toString().equalsIgnoreCase("child"))) {
			Map<String, Object> parentUser = (Map<String, Object>) eventMap.get("parentUser");
			userCo.setParentAccountUserName((parentUser.containsKey("username") && parentUser.get("username") != null) ? parentUser.get("username").toString() : null);
			userCo.setParentUid((parentUser.containsKey("gooruUId") && parentUser.get("gooruUId") != null) ? parentUser.get("gooruUId").toString() : null);
			userCo.setEmailId((eventMap.containsKey("username") && eventMap.get("username") != null) ? eventMap.get("username").toString() : null);
		} else {
			userCo.setParentAccountUserName("");
		}
		
		userCo.setLoginType((eventMap.containsKey("loginType") && eventMap.get("loginType") != null) ? eventMap.get("loginType").toString() : null);
		//userCo.setAccountTypeId((eventMap.containsKey("accountTypeId") && eventMap.get("accountTypeId") != null) ? Long.valueOf(eventMap.get("accountTypeId").toString()) : null);
		
		return userCo;
	}
	
	private UserCo getUser(Map<String, Object> user) {
		Map<String, Object> userPo = user;
		UserCo userCo = new UserCo();
		userCo.setUsername(userPo.get("username").toString());
		userCo.setDisplayname(userPo.get("usernameDisplay").toString());
		userCo.setFirstname(userPo.get("firstName").toString());
		userCo.setLastname(userPo.get("lastName").toString());

		String profileVisibility = "false";
		if (userPo.containsKey("customFields") && userPo.get("customFields") != null) {
			JSONSerializer customFieldsSerializer = new JSONSerializer();
			String serializedPCF = customFieldsSerializer.serialize(userPo.get("customFields"));
			List<PartyCustomFieldPo> partyCustomFieldPo = JSONDeserializer.deserialize(serializedPCF, new TypeReference<List<PartyCustomFieldPo>>() {
			});
			if (partyCustomFieldPo != null && partyCustomFieldPo.size() > 0) {
				for (PartyCustomFieldPo partyCustomField : partyCustomFieldPo) {
					if (partyCustomField.getOptionalKey().equalsIgnoreCase("show_profile_page")) {
						profileVisibility = partyCustomField.getOptionalValue();
						break;
					}
				}
			}
		}
		userCo.setProfileVisibility(profileVisibility);
		return userCo;
	}

	@SuppressWarnings("unchecked")
	public void updateAssessmentAnswer(Map<String, Object> eventMap, Map<String, Object> assessmentAnswerMap) {

		if (eventMap.containsKey("answers") && !(eventMap.get("typeName").toString().equalsIgnoreCase("OE"))) {
			List<Map<String, Object>> answerList = (List<Map<String, Object>>) eventMap.get("answers");
			if (answerList.size() > 0) {
				for (int index = 0; index < answerList.size(); index++) {
					try {
						if (assessmentAnswerMap.get("collectionGooruOid") != null && assessmentAnswerMap.get("questionGooruOid") != null && answerList.get(index).get("sequence") != null) {
							Long answerId = Long.valueOf(answerList.get(index).get("answerId").toString());
							String answerText = (eventMap.containsKey("answerText") && eventMap.get("answerText") != null) ? eventMap.get("answerText").toString() : null;
							String answerHint = null;
							if (eventMap.containsKey("hints")) {
								List<Map<String, Object>> hintList = (List<Map<String, Object>>) eventMap.get("hints");
								answerHint = (hintList.size() > 0 && hintList.get(0).containsKey("hintText")) ? hintList.get(0).get("hintText").toString() : null;
							}
							String answerExplanation = ((eventMap.containsKey("explanation") && eventMap.get("explanation") != null) ? eventMap.get("explanation").toString() : null);
							String questionType = ((eventMap.containsKey("typeName") && eventMap.get("typeName") != null) ? eventMap.get("typeName").toString() : null);
							assessmentAnswerMap.put("assesmentAnswerKey",
									assessmentAnswerMap.get("collectionGooruOid") + "~" + assessmentAnswerMap.get("questionGooruOid") + "~" + answerList.get(index).get("sequence"));
							assessmentAnswerMap.put("collectionGooruOid", assessmentAnswerMap.get("collectionGooruOid"));
							assessmentAnswerMap.put("questionGooruOid", assessmentAnswerMap.get("questionGooruOid"));
							assessmentAnswerMap.put("questionId", assessmentAnswerMap.get("questionId"));
							assessmentAnswerMap.put("answerId", answerId);
							assessmentAnswerMap.put("answerText", answerText);
							assessmentAnswerMap.put("isCorrect", (Integer.valueOf(Boolean.valueOf(answerList.get(index).get("isCorrect").toString()) ? 1 : 0)));
							assessmentAnswerMap.put("sequence", Integer.valueOf(answerList.get(index).get("sequence").toString()));

							assessmentAnswerMap.put("typeName",
									((answerList.get(index).containsKey("answerType") && answerList.get(index).get("answerType") != null) ? answerList.get(index).get("answerType").toString() : null));
							assessmentAnswerMap.put("answerHint", answerHint);
							assessmentAnswerMap.put("answerExplanation", answerExplanation);
							assessmentAnswerMap.put("questionType", questionType);
							assessmentAnswerMap.put("collectionContentId", assessmentAnswerMap.get("collectionContentId"));
						}
						baseCassandraDao.updateAssessmentAnswer(ColumnFamily.ASSESSMENTANSWER.getColumnFamily(), assessmentAnswerMap);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	public void updateCollectionTable(Map<String, Object> eventMap, Map<String, Object> collectionMap) {

		for(String field : COLLECTION_TABLE_FIELDS.split(",")) {
			if (field.equalsIgnoreCase("contentId") || field.equalsIgnoreCase("gooruOid")) {
				collectionMap.put(field, ((collectionMap.containsKey(field) && collectionMap.get(field) != null) ? collectionMap.get(field).toString()
						: ((eventMap.containsKey(field) && eventMap.get(field) != null) ? eventMap.get(field).toString() : null)));
			} else { 
					collectionMap.put(field, (eventMap.containsKey(field) && eventMap.get(field) != null) ? eventMap.get(field).toString() : null);
			}
			
		}
		if (collectionMap.containsKey("gooruOid") && collectionMap.get("gooruOid") != null) {
			collectionMap.put("rKey", collectionMap.get("gooruOid").toString());
			Set<Entry<String, String>> collectionCFKeySet = DataUtils.collectionCFKeys.entrySet();
			updateColumnFamily(ColumnFamily.COLLECTION.getColumnFamily(), this.generateCFMap(ColumnFamily.COLLECTION.getColumnFamily(), collectionCFKeySet, collectionMap));
		}

	}

	public void updateCollectionItemTable(Map<String, Object> eventMap, Map<String, Object> collectionItemMap) {
		
		if (collectionItemMap.get(COLLECTIONITEMID) != null) {
			Set<Entry<String, String>> entrySet = DataUtils.collectionItemTableKeys.entrySet();
			for(Entry<String, String> entry : entrySet) {
				if (entry.getKey().equalsIgnoreCase("questionType")) {
					collectionItemMap.put(entry.getValue(), ((collectionItemMap.containsKey(entry.getValue()) && collectionItemMap.get(entry.getValue()) != null) ? collectionItemMap.get(entry.getValue())
							.toString() : ((eventMap.containsKey("typeName") && eventMap.get("typeName") != null) ? eventMap.get("typeName").toString() : null)));
				} else if (entry.getKey().equalsIgnoreCase("collectionGooruOid") || entry.getKey().equalsIgnoreCase("resourceGooruOid") || entry.getKey().equalsIgnoreCase("collectionContentId") || entry.getKey().equalsIgnoreCase("resourceContentId") || entry.getKey().equalsIgnoreCase("deleted")) {
					collectionItemMap.put(entry.getValue(), (collectionItemMap.containsKey(entry.getKey()) && collectionItemMap.get(entry.getKey()) != null) ? collectionItemMap.get(entry.getKey()).toString() : null);
				} else if (entry.getKey().equalsIgnoreCase("associationDate") || entry.getKey().equalsIgnoreCase("associatedByUid")) {
					collectionItemMap.put(entry.getValue(), ((collectionItemMap.containsKey(entry.getValue()) && collectionItemMap.get(entry.getValue()) != null) ? collectionItemMap.get(entry.getValue()).toString() : ((eventMap.containsKey(entry.getValue()) && eventMap.get(entry.getValue()) != null) ? eventMap.get(entry.getValue()).toString() : null)));
				} else {
					collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
				}
			}
			collectionItemMap.put("rKey", collectionItemMap.get("collectionItemId").toString());
			Set<Entry<String, String>> collectionItemCFKeySet = DataUtils.collectionItemCFKeys.entrySet();
			updateColumnFamily(ColumnFamily.COLLECTIONITEM.getColumnFamily(), this.generateCFMap(ColumnFamily.COLLECTIONITEM.getColumnFamily(), collectionItemCFKeySet, collectionItemMap));
		} else {
			logger.info("Collection Item CF is not updated. CollectionItemId is null for event : {}", eventMap.get(EVENTNAME));
		}
	}

	public void updateClasspage(Map<String, Object> dataMap, Map<String, Object> classpageMap) {

		classpageMap.put("classId", ((dataMap.containsKey("gooruOid") && dataMap.get("gooruOid") != null) ? dataMap.get("gooruOid").toString() : null));
		classpageMap.put("username",
				((dataMap.containsKey("user") && ((Map<String, String>) dataMap.get("user")).get("username") != null) ? ((Map<String, String>) dataMap.get("user")).get("username") : null));
		classpageMap.put("userUid", ((classpageMap.containsKey("userUid") && classpageMap.get("userUid") != null) ? classpageMap.get("userUid") : (dataMap.containsKey("user") && ((Map<String, String>) dataMap.get("user")).get("gooruUId") != null) ? ((Map<String, String>) dataMap.get("user")).get("gooruUId")
				: null));
		if (classpageMap.get("classId") != null && classpageMap.get("groupUid") != null && classpageMap.get("userUid") != null) {

			classpageMap.put("rKey", classpageMap.get("classId").toString() + SEPERATOR + classpageMap.get("groupUid").toString() + SEPERATOR + classpageMap.get("userUid").toString());

			Set<Entry<String, String>> entrySet = DataUtils.classpageTableKeyMap.entrySet();
			updateColumnFamily(ColumnFamily.CLASSPAGE.getColumnFamily(), this.generateCFMap(ColumnFamily.CLASSPAGE.getColumnFamily(), entrySet, classpageMap));

		}
	}
	
	private Map<String, Object> generateCFMap(String cfName, Set<Entry<String, String>> entrySetToInsert, Map<String, Object> eventMap) {
		Map<String, Object> mapToInsert = new HashMap<String, Object>();
		Set<Entry<String, String>> dataTypeEntrySet = null;
		if(cfName.equalsIgnoreCase(ColumnFamily.CLASSPAGE.getColumnFamily())) {
			dataTypeEntrySet = DataUtils.classpageCFDataTypeMap.entrySet();
		} else if(cfName.equalsIgnoreCase(ColumnFamily.COLLECTION.getColumnFamily())) {
			dataTypeEntrySet = DataUtils.collectionCFDataType.entrySet();
		} else if(cfName.equalsIgnoreCase(ColumnFamily.COLLECTIONITEM.getColumnFamily())) {
			dataTypeEntrySet = DataUtils.collectionItemCFDataType.entrySet();
		}
		if(dataTypeEntrySet != null) {
			for(Entry<String, String> entry : entrySetToInsert) {
				if (eventMap.get(entry.getValue()) != null) {
					for(Entry<String, String> dataTypeEntry : dataTypeEntrySet) {
						if(dataTypeEntry.getKey().equalsIgnoreCase(entry.getKey())) {
							mapToInsert.put(entry.getKey(), TypeConverter.stringToAny((eventMap.get(entry.getValue()) != null ? eventMap.get(entry.getValue()).toString() : null), dataTypeEntry.getValue()));
						}
					}
				}
			}
		}
		return mapToInsert;
	}
	
	private Map<String, Object> generateCFMap(Set<Entry<String, String>> entrySet, Map<String, Object> eventMap) {
		Map<String, Object> mapToInsert = new HashMap<String, Object>();
		for(Entry<String, String> entry : entrySet) {
			if (eventMap.get(entry.getValue()) != null) {
				mapToInsert.put(entry.getKey(), eventMap.get(entry.getValue()) != null ? eventMap.get(entry.getValue()).toString() : null);
			}
		}
		return mapToInsert;
	}

	private void updateColumnFamily(String cfName, Map<String,Object> mapToInsert){
		if(mapToInsert.get("rKey") != null) {
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
			ColumnListMutation<String> cfm = m.withRow(baseCassandraDao.accessColumnFamily(cfName), mapToInsert.get("rKey").toString());
			for(Entry<String, Object> entry : mapToInsert.entrySet()){
				if(!entry.getKey().equalsIgnoreCase("rKey")) {
					baseCassandraDao.generateNonCounter(entry.getKey(), entry.getValue(), cfm);
				}
			}
			try {
				m.execute();
			} catch (ConnectionException e) {
				e.printStackTrace();
			}
		} else {
			logger.info("Incoming Key is null to update CF {}" ,cfName );
		}
	}
}
