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
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

public class GitlabConfiguration extends AbstractConfiguration {

    private String hostUrl;
    private String apiToken;
    private boolean ignoreCertificateErrors = false;

    @Override
    public void validate() {
    	if (StringUtil.isBlank(hostUrl)) {
    		throw new ConfigurationException("host.blank");
    	}
    	if (StringUtil.isBlank(apiToken)) {
    		throw new ConfigurationException("token.blank");
    	}
    }

    @ConfigurationProperty(displayMessageKey = "gitlab.config.hostUrl",
            helpMessageKey = "gitlab.config.hostUrl.help")
    public String getHostUrl() {
        return hostUrl;
    }

    public void setHostUrl(String hostUrl) {
        this.hostUrl = hostUrl;
    }

    @ConfigurationProperty(displayMessageKey = "gitlab.config.apiToken",
            helpMessageKey = "gitlab.config.apiToken.help")
    public String getApiToken() {
        return apiToken;
    }

    public void setApiToken(String apiToken) {
        this.apiToken = apiToken;
    }

    @ConfigurationProperty(displayMessageKey = "gitlab.config.ignoreCertificateErrors",
            helpMessageKey = "gitlab.config.ignoreCertificateErrors.help")
    public boolean getIgnoreCertificateErrors() {
        return ignoreCertificateErrors;
    }

    public void setIgnoreCertificateErrors(boolean ignoreCertificateErrors) {
        this.ignoreCertificateErrors = ignoreCertificateErrors;
    }
}