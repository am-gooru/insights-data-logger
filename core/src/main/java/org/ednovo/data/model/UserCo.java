package org.ednovo.data.model;

import java.util.Date;

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
	private String organizationUid;
	@Column
	private String emailId;
	@Column
	private String userName;
	@Column
	private String profileVisibility;
	@Column
	private Date createdOn;
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
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
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
}
