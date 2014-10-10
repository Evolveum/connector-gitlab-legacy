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
import org.gitlab.api.models.GitlabUser;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationalAttributeInfos;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.AttributeInfo.Flags;
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
		
		builder.defineObjectClass(objClassBuilder.build());
		
		return builder.build();
	}


	@Override
	public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> attributes, OperationOptions options) {
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
		List<GitlabUser> gitlabUsers;
		try {
			gitlabUsers = gitlabAPI.getUsers();
		} catch (IOException e) {
			throw new ConnectorIOException(e);
		}
		for (GitlabUser gitlabUser: gitlabUsers) {
			ConnectorObject connectorObject = convertUserToConnectorObject(gitlabUser);
			resultHandler.handle(connectorObject);
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

	private <T> void addAttr(ConnectorObjectBuilder builder, String attrName, T attrVal) {
		if (attrVal != null) {
			builder.addAttribute(attrName,attrVal);
		}
	}

	@Override
	public void delete(ObjectClass objectClass, Uid uid, OperationOptions options) {
		try {
			gitlabAPI.deleteUser(toInteger(uid));
		} catch (IOException e) {
			throw new ConnectorIOException(e.getMessage(), e);
		}
	}

	private Integer toInteger(Uid uid) {
		return Integer.parseInt(uid.getUidValue());
	}

	@Override
	public Uid create(ObjectClass objectClass, Set<Attribute> attributes, OperationOptions options) {
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
