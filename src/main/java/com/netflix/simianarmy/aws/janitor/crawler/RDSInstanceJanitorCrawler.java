/*
 *
 *  Copyright 2012 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.simianarmy.aws.janitor.crawler;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.ListTagsForResourceRequest;
import com.amazonaws.services.rds.model.ListTagsForResourceResult;
import com.amazonaws.services.rds.model.Tag;
import com.netflix.simianarmy.Resource;
import com.netflix.simianarmy.ResourceType;
import com.netflix.simianarmy.aws.AWSResource;
import com.netflix.simianarmy.aws.AWSResourceType;
import com.netflix.simianarmy.client.aws.AWSClient;

/**
 * The crawler to crawl AWS instances for janitor monkey.
 */
public class RDSInstanceJanitorCrawler extends AbstractAWSJanitorCrawler {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(RDSInstanceJanitorCrawler.class);

    /**
     * Instantiates a new basic instance crawler.
     * @param awsClient
     *            the aws client
     */
    public RDSInstanceJanitorCrawler(AWSClient awsClient) {
        super(awsClient);
    }

    @Override
    public EnumSet<? extends ResourceType> resourceTypes() {
        return EnumSet.of(AWSResourceType.RDS_INSTANCE);
    }

    @Override
    public List<Resource> resources(ResourceType resourceType) {
        if ("RDS_INSTANCE".equals(resourceType.name())) {
            return getDBInstanceResources();
        }
        return Collections.emptyList();
    }

    @Override
    public List<Resource> resources(String... resourceIds) {
        return getDBInstanceResources(resourceIds);
    }

    private List<Resource> getDBInstanceResources(String... dbInstanceIds) {
        List<Resource> resources = new LinkedList<Resource>();

        AWSClient awsClient = getAWSClient();

        for (DBInstance dbInstance : awsClient.describeDBInstances(dbInstanceIds)) {
            Resource dbInstanceResource = new AWSResource().withId(dbInstance.getDBInstanceIdentifier())
                    .withRegion(getAWSClient().region()).withResourceType(AWSResourceType.RDS_INSTANCE)
                    .withLaunchTime(dbInstance.getInstanceCreateTime());
            
            for (Tag tag : awsClient.listTagsForDBInstance(dbInstance.getDBInstanceIdentifier())) {
            	dbInstanceResource.setTag(tag.getKey(), tag.getValue());
            }
            String description = String.format("type=%s; engine=%s; host=%s", dbInstance.getDBInstanceClass(),
            		dbInstance.getEngine(), dbInstance.getDBInstanceIdentifier());
            dbInstanceResource.setDescription(description);
            dbInstanceResource.setOwnerEmail(getOwnerEmailForResource(dbInstanceResource));
            if (dbInstance.getDBInstanceStatus() != null) {
                ((AWSResource) dbInstanceResource).setAWSResourceState(dbInstance.getDBInstanceStatus());
            }
            resources.add(dbInstanceResource);
        }
        return resources;
    }

}
