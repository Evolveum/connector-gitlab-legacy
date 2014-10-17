/*
 * Copyright (c) 2014 Evolveum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.evolveum.polygon.connector.gitlab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.gitlab.api.GitlabAPI;
import org.gitlab.api.models.GitlabAccessLevel;
import org.gitlab.api.models.GitlabGroup;
import org.gitlab.api.models.GitlabGroupMember;
import org.gitlab.api.models.GitlabProject;
import org.gitlab.api.models.GitlabProjectMember;
import org.gitlab.api.models.GitlabUser;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationalAttributeInfos;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.AbstractFilterTranslator;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.CreateOp;
import org.identityconnectors.framework.spi.operations.DeleteOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.identityconnectors.framework.spi.operations.TestOp;
import org.identityconnectors.framework.spi.operations.UpdateOp;
import org.identityconnectors.framework.common.exceptions.*;

@ConnectorClass(displayNameKey = "gitlab.connector.display", configurationClass = GitlabConfiguration.class)
public class GitlabConnector implements Connector, CreateOp, DeleteOp, SchemaOp, SearchOp<String>, TestOp, UpdateOp {

    private static final Log LOG = Log.getLog(GitlabConnector.class);

    private static final String OBJECT_CLASS_PROJECT_NAME = "Project";
    
	private static final String ATTR_EMAIL = "email";
	private static final String ATTR_FULL_NAME = "fullName";
	private static final String ATTR_SKYPE_ID = "skypeId";
	private static final String ATTR_LINKED_ID = "linkedId";
	private static final String ATTR_TWITTER = "twitter";
	private static final String ATTR_WEBSITE_URL = "websiteUrl";
	private static final String ATTR_PROJECTS_LIMIT = "projectsLimit";
	private static final String ATTR_EXTERN_UID = "externUid";
	private static final String ATTR_EXTERN_PROVIDER_NAME = "externProviderName";
	private static final String ATTR_BIO = "bio";
	private static final String ATTR_IS_ADMIN = "isAdmin";
	private static final String ATTR_CAN_CREATE_GROUP = "canCreateGroup";
	private static final String ATTR_SKIP_CONFIRMATION = "skipConfirmation";
	private static final String ATTR_PATH = "path";
	private static final String ATTR_MEMBER = "member";
	private static final String ATTR_DEFAULT_BRANCH = "defaultBranch";
	private static final String ATTR_DESCRIPTION = "description";
	private static final String ATTR_HTTP_URL = "httpUrl";
	private static final String ATTR_NAMESPACE = "namespace";
	private static final String ATTR_OWNER = "owner";
	private static final String ATTR_SSH_URL = "sshUrl";
	private static final String ATTR_VISIBILITY_LEVEL = "visibilityLevel";
	private static final String ATTR_WEB_URL = "webUrl";
	private static final String ATTR_ISSUES_ENABLED = "issuesEnabled";
	private static final String ATTR_WALL_ENABLED = "wallEnabled";
	private static final String ATTR_MERGE_REQUESTS_ENABLED = "requestsEnabled";
	private static final String ATTR_WIKI_ENABLED = "wikiEnabled";
	private static final String ATTR_SNIPPETS_ENABLED = "snippetsEnabled";
	private static final String ATTR_PUBLIC = "public";
	private static final String ATTR_IMPORT_URL = "importUrl";

    private GitlabConfiguration configuration;
    private GitlabAPI gitlabAPI;

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {
        this.configuration = (GitlabConfiguration)configuration;
        gitlabAPI = GitlabAPI.connect(this.configuration.getHostUrl(),
        		this.configuration.getApiToken());
    }
    
	@Override
	public Schema schema() {
		SchemaBuilder builder = new SchemaBuilder(GitlabConnector.class);
		
		builder.defineObjectClass(schemaAccount());
		builder.defineObjectClass(schemaGroup());
		builder.defineObjectClass(schemaProject());
		
		return builder.build();
	}


	private ObjectClassInfo schemaAccount() {
		ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
		
		AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(ATTR_EMAIL);
		attrBuilder.setRequired(true);
		objClassBuilder.addAttributeInfo(attrBuilder.build());
		
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_FULL_NAME).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_SKYPE_ID).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_LINKED_ID).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_TWITTER).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_WEBSITE_URL).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_PROJECTS_LIMIT, Integer.class).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_EXTERN_UID).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_EXTERN_PROVIDER_NAME).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_BIO).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_IS_ADMIN, Boolean.class).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_CAN_CREATE_GROUP, Boolean.class).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_SKIP_CONFIRMATION, Boolean.class).build());
		// __PASSWORD__ attribute
        objClassBuilder.addAttributeInfo(OperationalAttributeInfos.PASSWORD);
		
		return objClassBuilder.build();
	}

	private ObjectClassInfo schemaGroup() {
		ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
		objClassBuilder.setType(ObjectClass.GROUP_NAME);
		
		AttributeInfoBuilder pathAttrBuilder = new AttributeInfoBuilder(ATTR_PATH);
		pathAttrBuilder.setRequired(true);
		pathAttrBuilder.setUpdateable(false);
		objClassBuilder.addAttributeInfo(pathAttrBuilder.build());

		AttributeInfoBuilder memberAttrBuilder = new AttributeInfoBuilder(ATTR_MEMBER, Integer.class);
		memberAttrBuilder.setMultiValued(true);
		objClassBuilder.addAttributeInfo(memberAttrBuilder.build());

		return objClassBuilder.build();
	}

	private ObjectClassInfo schemaProject() {
		ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
		objClassBuilder.setType(OBJECT_CLASS_PROJECT_NAME);
		
		AttributeInfoBuilder namespaceAttrBuilder = new AttributeInfoBuilder(ATTR_NAMESPACE, Integer.class);
		namespaceAttrBuilder.setRequired(true);
		namespaceAttrBuilder.setUpdateable(false);
		objClassBuilder.addAttributeInfo(namespaceAttrBuilder.build());
		
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_PATH).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_DEFAULT_BRANCH).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_DESCRIPTION).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_HTTP_URL).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_OWNER).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_SSH_URL).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_VISIBILITY_LEVEL, Integer.class).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_WEB_URL).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_ISSUES_ENABLED, Boolean.class).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_WALL_ENABLED, Boolean.class).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_MERGE_REQUESTS_ENABLED, Boolean.class).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_WIKI_ENABLED, Boolean.class).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_SNIPPETS_ENABLED, Boolean.class).build());
		objClassBuilder.addAttributeInfo(new AttributeInfoBuilder(ATTR_PUBLIC, Boolean.class).build());

		AttributeInfoBuilder memberAttrBuilder = new AttributeInfoBuilder(ATTR_MEMBER, Integer.class);
		memberAttrBuilder.setMultiValued(true);
		objClassBuilder.addAttributeInfo(memberAttrBuilder.build());

		return objClassBuilder.build();
	}

	@Override
	public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> attributes, OperationOptions options) {
		if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
			return updateUser(uid, attributes, options);
		} else if (objectClass.is(ObjectClass.GROUP_NAME)) {
			try {
				return updateGroup(uid, attributes, options);
			} catch (IOException e) {
				throw new ConnectorIOException(e.getMessage(), e);
			}
		} else if (objectClass.is(OBJECT_CLASS_PROJECT_NAME)) {
			try {
				return updateProject(uid, attributes, options);
			} catch (IOException e) {
				throw new ConnectorIOException(e.getMessage(), e);
			}
		} else {
			throw new UnsupportedOperationException("Unsupported object class "+objectClass);
		}
	}
	
	private Uid updateUser(Uid uid, Set<Attribute> attributes, OperationOptions options) {
		Integer targetUserId = toInteger(uid);
		
		GitlabUser origUser;
		try {
			origUser = gitlabAPI.getUser(targetUserId);
		} catch (IOException e) {
			throw new ConnectorIOException(e.getMessage(), e);
		}
		if (origUser == null) {
			throw new UnknownUidException("User with ID "+targetUserId+" does not exist");
		}
		
		String email = getStringAttr(attributes, ATTR_EMAIL, origUser.getEmail());

		// I hate this GuardedString! 
		final List<String> passwordList = new ArrayList<String>(1);
		GuardedString guardedPassword = getAttr(attributes, OperationalAttributeInfos.PASSWORD.getName(), GuardedString.class);
		if (guardedPassword != null) {
			guardedPassword.access(new GuardedString.Accessor() {
				@Override
				public void access(char[] chars) {
					passwordList.add(new String(chars));
				}
			});
		}
		String password = null;
		if (!passwordList.isEmpty()) {
			password = passwordList.get(0);
		}
		
		String username = getStringAttr(attributes, Name.NAME, origUser.getUsername());
		String fullName = getStringAttr(attributes, ATTR_FULL_NAME, origUser.getName());
		String skypeId = getStringAttr(attributes, ATTR_SKYPE_ID, origUser.getSkype());
		String linkedIn = getStringAttr(attributes, ATTR_LINKED_ID, origUser.getLinkedin());
		String twitter = getStringAttr(attributes, ATTR_TWITTER, origUser.getTwitter());
		String website_url = getStringAttr(attributes, ATTR_WEBSITE_URL, origUser.getWebsiteUrl());
		Integer projects_limit = getAttr(attributes, ATTR_PROJECTS_LIMIT, Integer.class);
		String extern_uid = getStringAttr(attributes, ATTR_EXTERN_UID, origUser.getExternUid());
		String extern_provider_name = getStringAttr(attributes, ATTR_EXTERN_PROVIDER_NAME, origUser.getExternProviderName());
		String bio = getStringAttr(attributes, ATTR_BIO, origUser.getBio());
		Boolean isAdmin = getAttr(attributes, ATTR_IS_ADMIN, Boolean.class, origUser.isAdmin());
		Boolean can_create_group = getAttr(attributes, ATTR_CAN_CREATE_GROUP, Boolean.class, origUser.isCanCreateGroup());
		Boolean skip_confirmation = getAttr(attributes, ATTR_SKIP_CONFIRMATION, Boolean.class);
		if (skip_confirmation == null) {
			skip_confirmation = Boolean.TRUE;
		}

		try {
			GitlabUser gitlabUser = gitlabAPI.updateUser(targetUserId, email, password, username, fullName, skypeId, linkedIn, twitter, website_url, projects_limit, extern_uid, extern_provider_name, bio, isAdmin, can_create_group, skip_confirmation);
		} catch (IOException e) {
			throw new ConnectorIOException(e.getMessage(), e);
		}
		
		return uid;
	}

	private Uid updateGroup(Uid uid, Set<Attribute> attributes, OperationOptions options) throws IOException {
		Integer targetId = toInteger(uid);

		GitlabGroup origGroup = gitlabAPI.getGroup(targetId);
		if (origGroup == null) {
			throw new UnknownUidException("Group with ID "+targetId+" does not exist");
		}
		
		String path = getStringAttr(attributes, ATTR_PATH, null);
		if (path != null) {
			throw new InvalidAttributeValueException("Group "+ATTR_PATH+" cannot be changed");
		}
		String name = getStringAttr(attributes, Name.NAME, null);
		if (name != null) {
			throw new InvalidAttributeValueException("Group "+Name.NAME+" cannot be changed");
		}

		for (Attribute attr: attributes) {
			if (ATTR_MEMBER.equals(attr.getName())) {
				List<Object> values = attr.getValue();
				List<Integer> newMemberIds = new ArrayList<Integer>(values.size());
				List<Integer> membersToAdd = new ArrayList<Integer>();
				List<Integer> membersToDelete = new ArrayList<Integer>();
				List<GitlabGroupMember> origMembers = gitlabAPI.getGroupMembers(origGroup);
				for (Object attrValue: values) {
					newMemberIds.add(Integer.parseInt((String) attrValue));
				}
				// Primitive. But effective.
				for (Integer newMemberId: newMemberIds) {
					boolean found = false;
					for (GitlabGroupMember origMember: origMembers) {
						if (origMember.getId() == newMemberId) {
							found = true;
							break;
						}
					}
					if (!found) {
						membersToAdd.add(newMemberId);
					}
				}
				for (GitlabGroupMember origMember: origMembers) {
					if (!newMemberIds.contains(origMember.getId())) {
						membersToDelete.add(origMember.getId());
					}
				}
				
				for (Integer memberId: membersToAdd) {
					gitlabAPI.addGroupMember(targetId, memberId, GitlabAccessLevel.Developer);
				}
				
				for (Integer memberId: membersToDelete) {
					gitlabAPI.deleteGroupMember(targetId, memberId);
				}
			}
		}
		
		return uid;
	}

	private Uid updateProject(Uid uid, Set<Attribute> attributes, OperationOptions options) throws IOException {
		Integer targetId = toInteger(uid);

		GitlabProject origProject = gitlabAPI.getProject(uid.getUidValue());
		if (origProject == null) {
			throw new UnknownUidException("Project with ID "+targetId+" does not exist");
		}
		
		String path = getStringAttr(attributes, ATTR_PATH, null);
		if (path != null) {
			throw new InvalidAttributeValueException("Project "+ATTR_PATH+" cannot be changed");
		}
		String name = getStringAttr(attributes, Name.NAME, null);
		if (name != null) {
			throw new InvalidAttributeValueException("Project "+Name.NAME+" cannot be changed");
		}
		// TODO: check for other non-changable attributes

		for (Attribute attr: attributes) {
			if (ATTR_MEMBER.equals(attr.getName())) {
				List<Object> values = attr.getValue();
				List<Integer> newMemberIds = new ArrayList<Integer>(values.size());
				List<Integer> membersToAdd = new ArrayList<Integer>();
				List<Integer> membersToDelete = new ArrayList<Integer>();
				List<GitlabProjectMember> origMembers = gitlabAPI.getProjectMembers(origProject);
				for (Object attrValue: values) {
					newMemberIds.add(Integer.parseInt((String) attrValue));
				}
				// Primitive. But effective.
				for (Integer newMemberId: newMemberIds) {
					boolean found = false;
					for (GitlabProjectMember origMember: origMembers) {
						if (origMember.getId() == newMemberId) {
							found = true;
							break;
						}
					}
					if (!found) {
						membersToAdd.add(newMemberId);
					}
				}
				for (GitlabProjectMember origMember: origMembers) {
					if (!newMemberIds.contains(origMember.getId())) {
						membersToDelete.add(origMember.getId());
					}
				}
				
				LOG.ok("MEMBERS: {0}\nnewMemberIds: {1}\norigMembers: {2}\nadd: {3}\ndelete:{4}", targetId, newMemberIds, origMembers, membersToAdd, membersToDelete);
				
				for (Integer memberId: membersToAdd) {
					LOG.ok("Adding account {0} to project {1}", targetId, memberId);
					gitlabAPI.addProjectMember(targetId, memberId, GitlabAccessLevel.Developer);
				}
				
				for (Integer memberId: membersToDelete) {
					LOG.ok("Deleting account {0} from project {1}", targetId, memberId);
					gitlabAPI.deleteProjectMember(targetId, memberId);
				}
			}
		}
		
		return uid;
	}

	@Override
	public void test() {
		try {
			gitlabAPI.getGroups();
		} catch (IOException e) {
			throw new ConnectorIOException(e.getMessage(), e);
		}
	}

	@Override
	public FilterTranslator<String> createFilterTranslator(ObjectClass objectClass, OperationOptions options) {
		return new AbstractFilterTranslator<String>() {
	       };
	}

	@Override
	public void executeQuery(ObjectClass objectClass, String query, ResultsHandler resultHandler, OperationOptions options) {
		if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
			List<GitlabUser> gitlabUsers;
			try {
				gitlabUsers = gitlabAPI.getUsers();
			} catch (IOException e) {
				throw new ConnectorIOException(e.getMessage(), e);
			}
			for (GitlabUser gitlabUser: gitlabUsers) {
				ConnectorObject connectorObject = convertUserToConnectorObject(gitlabUser);
				resultHandler.handle(connectorObject);
			}
			
		} else if (objectClass.is(ObjectClass.GROUP_NAME)) {
			List<GitlabGroup> gitlabGroups;
			try {
				gitlabGroups = gitlabAPI.getGroups();
			} catch (IOException e) {
				throw new ConnectorIOException(e.getMessage(), e);
			}
			for (GitlabGroup gitlabGroup: gitlabGroups) {
				ConnectorObject connectorObject = convertGroupToConnectorObject(gitlabGroup);
				resultHandler.handle(connectorObject);
			}
		} else if (objectClass.is(OBJECT_CLASS_PROJECT_NAME)) {
			List<GitlabProject> gitlabProjects;
			try {
				gitlabProjects = gitlabAPI.getAllProjects();
			} catch (IOException e) {
				throw new ConnectorIOException(e.getMessage(), e);
			}
			LOG.ok("GITLAB projects: "+gitlabProjects);
			for (GitlabProject gitlabProject: gitlabProjects) {
				ConnectorObject connectorObject = convertProjectToConnectorObject(gitlabProject);
				resultHandler.handle(connectorObject);
			}			
		} else {
			throw new UnsupportedOperationException("Unsupported object class "+objectClass);
		}
	}

	private ConnectorObject convertUserToConnectorObject(GitlabUser gitlabUser) {
		ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
		builder.setUid(gitlabUser.getId().toString());
		builder.setName(gitlabUser.getUsername());
		addAttr(builder,ATTR_EMAIL, gitlabUser.getEmail());
		addAttr(builder,ATTR_FULL_NAME, gitlabUser.getName());
		addAttr(builder,ATTR_SKYPE_ID, gitlabUser.getSkype());
		addAttr(builder,ATTR_LINKED_ID, gitlabUser.getLinkedin());
		addAttr(builder,ATTR_TWITTER, gitlabUser.getTwitter());
		addAttr(builder,ATTR_WEBSITE_URL, gitlabUser.getWebsiteUrl());
		addAttr(builder,ATTR_EXTERN_UID, gitlabUser.getExternUid());
		addAttr(builder,ATTR_EXTERN_PROVIDER_NAME, gitlabUser.getExternProviderName());
		addAttr(builder,ATTR_BIO, gitlabUser.getBio());
		addAttr(builder,ATTR_IS_ADMIN, gitlabUser.isAdmin());
		addAttr(builder,ATTR_CAN_CREATE_GROUP, gitlabUser.isCanCreateGroup());
		return builder.build();
	}

	private ConnectorObject convertGroupToConnectorObject(GitlabGroup gitlabGroup) {
		ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
		builder.setObjectClass(ObjectClass.GROUP);
		builder.setUid(gitlabGroup.getId().toString());
		builder.setName(gitlabGroup.getName());
		addAttr(builder,ATTR_PATH, gitlabGroup.getPath());
		
		AttributeBuilder memberAttrBuilder = new AttributeBuilder();
		memberAttrBuilder.setName(ATTR_MEMBER);
		List<GitlabGroupMember> groupMembers;
		try {
			groupMembers = gitlabAPI.getGroupMembers(gitlabGroup);
		} catch (IOException e) {
			throw new ConnectorIOException(e.getMessage(), e);
		}
		if (groupMembers != null && !groupMembers.isEmpty()) {
			for (GitlabGroupMember gitlabMember: groupMembers) {
				Integer id = gitlabMember.getId();
				memberAttrBuilder.addValue(id);
			}
			builder.addAttribute(memberAttrBuilder.build());
		}
		
		return builder.build();
	}

	private ConnectorObject convertProjectToConnectorObject(GitlabProject gitlabProject) {
		ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
		builder.setObjectClass(new ObjectClass(OBJECT_CLASS_PROJECT_NAME));
		builder.setUid(gitlabProject.getId().toString());
		builder.setName(gitlabProject.getName());
		addAttr(builder,ATTR_PATH, gitlabProject.getPath());
		addAttr(builder,ATTR_DEFAULT_BRANCH, gitlabProject.getDefaultBranch());
		addAttr(builder,ATTR_DESCRIPTION, gitlabProject.getDescription());
		addAttr(builder,ATTR_HTTP_URL, gitlabProject.getHttpUrl());
		addAttr(builder,ATTR_NAMESPACE, gitlabProject.getNamespace().getId());
		GitlabUser owner = gitlabProject.getOwner();
		if (owner != null) {
			addAttr(builder,ATTR_OWNER, owner.getId());
		}
		addAttr(builder,ATTR_SSH_URL, gitlabProject.getSshUrl());
		addAttr(builder,ATTR_VISIBILITY_LEVEL, gitlabProject.getVisibilityLevel());
		addAttr(builder,ATTR_WEB_URL, gitlabProject.getWebUrl());
		addAttr(builder,ATTR_ISSUES_ENABLED, gitlabProject.isIssuesEnabled());
		addAttr(builder,ATTR_MERGE_REQUESTS_ENABLED, gitlabProject.isMergeRequestsEnabled());
		addAttr(builder,ATTR_PUBLIC, gitlabProject.isPublic());
		addAttr(builder,ATTR_SNIPPETS_ENABLED, gitlabProject.isSnippetsEnabled());
		addAttr(builder,ATTR_WALL_ENABLED, gitlabProject.isWallEnabled());
		addAttr(builder,ATTR_WIKI_ENABLED, gitlabProject.isWikiEnabled());
		
		AttributeBuilder memberAttrBuilder = new AttributeBuilder();
		memberAttrBuilder.setName(ATTR_MEMBER);
		List<GitlabProjectMember> members;
		try {
			members = gitlabAPI.getProjectMembers(gitlabProject);
		} catch (IOException e) {
			throw new ConnectorIOException(e.getMessage(), e);
		}
		if (members != null && !members.isEmpty()) {
			for (GitlabProjectMember gitlabMember: members) {
				Integer id = gitlabMember.getId();
				memberAttrBuilder.addValue(id);
			}
			builder.addAttribute(memberAttrBuilder.build());
		}
		
		return builder.build();
	}
	
	private <T> void addAttr(ConnectorObjectBuilder builder, String attrName, T attrVal) {
		if (attrVal != null) {
			builder.addAttribute(attrName,attrVal);
		}
	}

	@Override
	public void delete(ObjectClass objectClass, Uid uid, OperationOptions options) {
		if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
			try {
				gitlabAPI.deleteUser(toInteger(uid));
			} catch (IOException e) {
				throw new ConnectorIOException(e.getMessage(), e);
			}
		} else if (objectClass.is(ObjectClass.GROUP_NAME)) {
			throw new UnsupportedOperationException("Deletion of group seems to be not supported by Gitlab API");
		} else if (objectClass.is(OBJECT_CLASS_PROJECT_NAME)) {
			throw new UnsupportedOperationException("Deletion of project seems to be not supported by Gitlab API");
		} else {
			throw new UnsupportedOperationException("Unsupported object class "+objectClass);
		}
	}

	private Integer toInteger(Uid uid) {
		return Integer.parseInt(uid.getUidValue());
	}

	@Override
	public Uid create(ObjectClass objectClass, Set<Attribute> attributes, OperationOptions options) {
		if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
			return createUser(attributes, options);
		} else if (objectClass.is(ObjectClass.GROUP_NAME)) {
			return createGroup(attributes, options);
		} else if (objectClass.is(OBJECT_CLASS_PROJECT_NAME)) {
			return createProject(attributes, options);
		} else {
			throw new UnsupportedOperationException("Unsupported object class "+objectClass);
		}
	}
			
	private Uid createUser(Set<Attribute> attributes, OperationOptions options) {
		String email = getStringAttr(attributes, ATTR_EMAIL);
		if (email == null) {
			throw new InvalidAttributeValueException("Missing mandatory attribute "+ATTR_EMAIL);
		}
		// I hate this GuardedString! 
		final List<String> passwordList = new ArrayList<String>(1);
		GuardedString guardedPassword = getAttr(attributes, OperationalAttributeInfos.PASSWORD.getName(), GuardedString.class);
		if (guardedPassword != null) {
			guardedPassword.access(new GuardedString.Accessor() {
				@Override
				public void access(char[] chars) {
					passwordList.add(new String(chars));
				}
			});
		}
		String password = null;
		if (!passwordList.isEmpty()) {
			password = passwordList.get(0);
		}
		
		String username = getStringAttr(attributes, Name.NAME);
		String fullName = getStringAttr(attributes, ATTR_FULL_NAME);
		String skypeId = getStringAttr(attributes, ATTR_SKYPE_ID);
		String linkedIn = getStringAttr(attributes, ATTR_LINKED_ID);
		String twitter = getStringAttr(attributes, ATTR_TWITTER);
		String website_url = getStringAttr(attributes, ATTR_WEBSITE_URL);
		Integer projects_limit = getAttr(attributes, ATTR_PROJECTS_LIMIT, Integer.class);
		String extern_uid = getStringAttr(attributes, ATTR_EXTERN_UID);
		String extern_provider_name = getStringAttr(attributes, ATTR_EXTERN_PROVIDER_NAME);
		String bio = getStringAttr(attributes, ATTR_BIO);
		Boolean isAdmin = getAttr(attributes, ATTR_IS_ADMIN, Boolean.class);
		Boolean can_create_group = getAttr(attributes, ATTR_CAN_CREATE_GROUP, Boolean.class);;
		Boolean skip_confirmation = getAttr(attributes, ATTR_SKIP_CONFIRMATION, Boolean.class);
		if (skip_confirmation == null) {
			skip_confirmation = Boolean.TRUE;
		}
		try {
			GitlabUser gitlabUser = gitlabAPI.createUser(email, password, username, fullName, 
					skypeId, linkedIn, twitter, website_url, projects_limit, 
					extern_uid, extern_provider_name, bio, isAdmin, 
					can_create_group, skip_confirmation);
			Integer id = gitlabUser.getId();
			return new Uid(id.toString());
		} catch (IOException e) {
			throw new ConnectorIOException(e.getMessage(), e);
		}
	}

	private Uid createGroup(Set<Attribute> attributes, OperationOptions options) {
		String name = getStringAttr(attributes, Name.NAME);
		String path = getStringAttr(attributes, ATTR_PATH);
		if (path == null) {
			throw new InvalidAttributeValueException("Missing mandatory attribute "+ATTR_PATH);
		}		
		try {
			GitlabGroup gitlabGroup = gitlabAPI.createGroup(name, path);
			Integer id = gitlabGroup.getId();
			return new Uid(id.toString());
		} catch (IOException e) {
			throw new ConnectorIOException(e.getMessage(), e);
		}
	}

	private Uid createProject(Set<Attribute> attributes, OperationOptions options) {
		String name = getStringAttr(attributes, Name.NAME);
		Integer namespaceId = getAttr(attributes, ATTR_NAMESPACE, Integer.class);
		if (namespaceId == null) {
			throw new InvalidAttributeValueException("Missing mandatory attribute "+ATTR_NAMESPACE);
		}
		String description = getStringAttr(attributes, ATTR_DESCRIPTION);
		Boolean issuesEnabled = getAttr(attributes, ATTR_ISSUES_ENABLED, Boolean.class);
		Boolean wallEnabled = getAttr(attributes, ATTR_WALL_ENABLED, Boolean.class);
		Boolean mergeRequestsEnabled = getAttr(attributes, ATTR_MERGE_REQUESTS_ENABLED, Boolean.class);
		Boolean wikiEnabled = getAttr(attributes, ATTR_WIKI_ENABLED, Boolean.class);
		Boolean snippetsEnabled = getAttr(attributes, ATTR_SNIPPETS_ENABLED, Boolean.class);
		Boolean publik = getAttr(attributes, ATTR_PUBLIC, Boolean.class);
		Integer visibilityLevel = getAttr(attributes, ATTR_VISIBILITY_LEVEL, Integer.class);
		String importUrl = getStringAttr(attributes, ATTR_IMPORT_URL);
		
		try {
			GitlabProject gitlabProject = gitlabAPI.createProject(name, namespaceId, description, issuesEnabled, wallEnabled, mergeRequestsEnabled, wikiEnabled, snippetsEnabled, publik, visibilityLevel, importUrl);
			Integer id = gitlabProject.getId();
			return new Uid(id.toString());
		} catch (IOException e) {
			throw new ConnectorIOException(e.getMessage(), e);
		}
	}

    private String getStringAttr(Set<Attribute> attributes, String attrName) throws InvalidAttributeValueException {
    	return getAttr(attributes, attrName, String.class);
    }
    
    private String getStringAttr(Set<Attribute> attributes, String attrName, String defaultVal) throws InvalidAttributeValueException {
    	return getAttr(attributes, attrName, String.class, defaultVal);
    }
    
    private <T> T getAttr(Set<Attribute> attributes, String attrName, Class<T> type) throws InvalidAttributeValueException {
    	return getAttr(attributes, attrName, type, null);
    }
    
	private <T> T getAttr(Set<Attribute> attributes, String attrName, Class<T> type, T defaultVal) throws InvalidAttributeValueException {
		for (Attribute attr: attributes) {
			if (attrName.equals(attr.getName())) {
				List<Object> vals = attr.getValue();
				if (vals == null || vals.isEmpty()) {
					return defaultVal;
				}
				if (vals.size() == 1) {
					Object val = vals.get(0);
					if (val == null) {
						return defaultVal;
					}
					if (type.isAssignableFrom(val.getClass())) {
						return (T)val;
					}
					throw new InvalidAttributeValueException("Unsupported type "+val.getClass()+" for attribute "+attrName);
				}
				throw new InvalidAttributeValueException("More than one value for attribute "+attrName);
			}
		}
		return defaultVal;
	}

	@Override
    public void dispose() {
        configuration = null;
        if (gitlabAPI != null) {
        	// GitlabAPI seems to not maintain any objects that
        	// need to be explicitly disposed of.
        	// So just let garbage collector do the work
            gitlabAPI = null;
        }
    }
}
