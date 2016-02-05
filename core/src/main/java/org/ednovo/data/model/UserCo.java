package org.ednovo.data.model;

import java.util.Date;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity(name = "user")
public class UserCo {

	@Id
	private String userUid;
	@Column
	private Integer userId;
	@Column
	private String displayname;
	@Column
	private String firstname;
	@Column	
	private String lastname;
	@Column
	private String emailId;
	@Column
	private String username;
	@Column
	private String profileVisibility;
	@Column
	private Date createdOn;
	@Column
    private String accountId;
    @Column
    private String confirmStatus;
    @Column
    private String userProfileImage;
    @Column
    private String roleSet;
	@Column
    private String grade;
    @Column
    private String network;
    @Column
    private Map<String,String> organization;
    @Column
    private String versionUid;
    @Column
    private Date lastLogin;
    @Column
    private boolean isDeleted;
    @Column
    private String accountRegisterType;
    @Column
    private String aboutMe;
    @Column
    private String notes;
    @Column
    private Date lastModifiedOn;
    @Column
    private String metaJson;
    @Column
    private Short active;
    @Column
    private String parentAccountUserName;
    
    @Column
    private String organizationUid;
    @Column
    private String gooruUid;
    @Column
    private String parentUid;
    @Column
    private String loginType;

	public String getUserUid() {
		return userUid;
	}
	public void setUserUid(String userUid) {
		this.userUid = userUid;
	}
	public Integer getUserId() {
		return userId;
	}
	public void setUserId(Integer userId) {
		this.userId = userId;
	}
	public String getDisplayname() {
		return displayname;
	}
	public void setDisplayname(String displayname) {
		this.displayname = displayname;
	}
	public String getFirstname() {
		return firstname;
	}
	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}
	public String getLastname() {
		return lastname;
	}
	public void setLastname(String lastname) {
		this.lastname = lastname;
	}
	public String getOrganizationUid() {
		return organizationUid;
	}
	public void setOrganizationUid(String organizationUid) {
		this.organizationUid = organizationUid;
	}
	public String getEmailId() {
		return emailId;
	}
	public void setEmailId(String emailId) {
		this.emailId = emailId;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getProfileVisibility() {
		return profileVisibility;
	}
	public void setProfileVisibility(String profileVisibility) {
		this.profileVisibility = profileVisibility;
	}
	public Date getCreatedOn() {
		return createdOn;
	}
	public void setCreatedOn(Date createdOn) {
		this.createdOn = createdOn;
	}

    public String getAccountId() {
		return accountId;
	}
	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}
	public String getConfirmStatus() {
		return confirmStatus;
	}
	public void setConfirmStatus(String confirmStatus) {
		this.confirmStatus = confirmStatus;
	}
	public String getUserProfileImage() {
		return userProfileImage;
	}
	public void setUserProfileImage(String userProfileImage) {
		this.userProfileImage = userProfileImage;
	}
	public String getRoleSet() {
		return roleSet;
	}
	public void setRoleSet(String roleSet) {
		this.roleSet = roleSet;
	}
	public String getGrade() {
		return grade;
	}
	public void setGrade(String grade) {
		this.grade = grade;
	}
	public String getNetwork() {
		return network;
	}
	public void setNetwork(String network) {
		this.network = network;
	}
	public Map<String, String> getOrganization() {
		return organization;
	}
	public void setOrganization(Map<String, String> organization) {
		this.organization = organization;
	}
	public String getVersionUid() {
		return versionUid;
	}
	public void setVersionUid(String versionUid) {
		this.versionUid = versionUid;
	}
	public Date getLastLogin() {
		return lastLogin;
	}
	public void setLastLogin(Date lastLogin) {
		this.lastLogin = lastLogin;
	}
	public boolean isDeleted() {
		return isDeleted;
	}
	public void setDeleted(boolean isDeleted) {
		this.isDeleted = isDeleted;
	}
	public String getAccountRegisterType() {
		return accountRegisterType;
	}
	public void setAccountRegisterType(String accountRegisterType) {
		this.accountRegisterType = accountRegisterType;
	}
	public String getAboutMe() {
		return aboutMe;
	}
	public void setAboutMe(String aboutMe) {
		this.aboutMe = aboutMe;
	}
	public String getNotes() {
		return notes;
	}
	public void setNotes(String notes) {
		this.notes = notes;
	}
	public Date getLastModifiedOn() {
		return lastModifiedOn;
	}
	public void setLastModifiedOn(Date lastModifiedOn) {
		this.lastModifiedOn = lastModifiedOn;
	}
	public String getMetaJson() {
		return metaJson;
	}
	public void setMetaJson(String metaJson) {
		this.metaJson = metaJson;
	}
	public Short getActive() {
		return active;
	}
	public void setActive(Short active) {
		this.active = active;
	}
	public String getParentAccountUserName() {
		return parentAccountUserName;
	}
	public void setParentAccountUserName(String parentAccountUserName) {
		this.parentAccountUserName = parentAccountUserName;
	}
	public void setGooruUid(String gooruUid) {
		this.gooruUid = gooruUid;
	}
	public String getGooruUid() {
		return gooruUid;
	}
	public void setParentUid(String parentUid) {
		this.parentUid = parentUid;
	}
	public String getParentUid() {
		return parentUid;
	}
	public void setLoginType(String loginType) {
		this.loginType = loginType;
	}
	public String getLoginType() {
		return loginType;
	}
}
