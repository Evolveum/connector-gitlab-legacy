package com.evolveum.polygon.connector.gitlab;

import org.testng.annotations.Test;

import junit.framework.Assert;

public class GitlabConnectorTest {

	GitlabConnector dummyGitlabConnector = new GitlabConnector();

	@Test
	public void retrieveUserIdFromUid() {
		String uid = "36|61";

		Assert.assertEquals(new Integer("36"), dummyGitlabConnector.getUserIdFromMemberOfUid(uid));
	}

	@Test
	public void retrieveGroupIdFromUid() {
		String uid = "36|61";

		Assert.assertEquals(new Integer("61"), dummyGitlabConnector.getGroupIdFromMemberOfUid(uid));
	}

	@Test
	public void generateUidFromUserIdAndGroupId() {
		Integer userId = 36;
		Integer groupId = 61;

		Assert.assertEquals("36|61", dummyGitlabConnector.assembleMemberOfUid(userId, groupId));
	}

}
