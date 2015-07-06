// CHECKSTYLE IGNORE Javadoc
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazonaws.services.rds.model.DBInstance;
import com.netflix.simianarmy.Resource;
import com.netflix.simianarmy.aws.AWSResource;
import com.netflix.simianarmy.aws.AWSResourceType;
import com.netflix.simianarmy.client.aws.AWSClient;

public class TestRDSInstanceJanitorCrawler {

    @Test
    public void testResourceTypes() {
        List<DBInstance> dbInstanceList = createInstanceList();
        RDSInstanceJanitorCrawler crawler = new RDSInstanceJanitorCrawler(createMockAWSClient(
                dbInstanceList));
        EnumSet<?> types = crawler.resourceTypes();
        Assert.assertEquals(types.size(), 1);
        Assert.assertEquals(types.iterator().next().name(), "RDS_INSTANCE");
    }

    @Test
    public void testInstancesWithNullIds() {
        List<DBInstance> dbInstanceList = createInstanceList();
        AWSClient awsMock = createMockAWSClient(dbInstanceList);
        RDSInstanceJanitorCrawler crawler = new RDSInstanceJanitorCrawler(awsMock);
        List<Resource> resources = crawler.resources();
        verifyInstanceList(resources, dbInstanceList);
    }

    @Test
    public void testInstancesWithIds() {
        List<DBInstance> dbInstanceList = createInstanceList();
        String[] ids = {"rds-db-1", "rds-db-2"};
        AWSClient awsMock = createMockAWSClient(dbInstanceList, ids);
        RDSInstanceJanitorCrawler crawler = new RDSInstanceJanitorCrawler(awsMock);
        List<Resource> resources = crawler.resources(ids);
        verifyInstanceList(resources, dbInstanceList);
    }

    @Test
    public void testInstancesWithResourceType() {
        List<DBInstance> dbInstanceList = createInstanceList();
        AWSClient awsMock = createMockAWSClient(dbInstanceList);
        RDSInstanceJanitorCrawler crawler = new RDSInstanceJanitorCrawler(awsMock);
        for (AWSResourceType resourceType : AWSResourceType.values()) {
            List<Resource> resources = crawler.resources(resourceType);
            if (resourceType == AWSResourceType.RDS_INSTANCE) {
                verifyInstanceList(resources, dbInstanceList);
            } else {
                Assert.assertTrue(resources.isEmpty());
            }
        }
    }

    private void verifyInstanceList(List<Resource> resources, List<DBInstance> dbInstanceList) {
        Assert.assertEquals(resources.size(), dbInstanceList.size());
        for (int i = 0; i < resources.size(); i++) {
            DBInstance instance = dbInstanceList.get(i);
            verifyInstance(resources.get(i), instance.getDBInstanceIdentifier());
        }
    }

    private void verifyInstance(Resource instance, String dbName) {
        Assert.assertEquals(instance.getResourceType(), AWSResourceType.RDS_INSTANCE);
        Assert.assertEquals(instance.getId(), dbName);
        Assert.assertEquals(instance.getRegion(), "us-east-1");
        Assert.assertEquals(((AWSResource) instance).getAWSResourceState(), "running");
    }

    private AWSClient createMockAWSClient(List<DBInstance> instanceList, String... dbNames) {
        AWSClient awsMock = mock(AWSClient.class);
        when(awsMock.describeDBInstances(dbNames)).thenReturn(instanceList);
        when(awsMock.region()).thenReturn("us-east-1");
        return awsMock;
    }


    private List<DBInstance> createInstanceList() {
        List<DBInstance> dbInstanceList = new LinkedList<DBInstance>();
        dbInstanceList.add(mkDBInstance("rds-db-1"));
        dbInstanceList.add(mkDBInstance("rds-db-2"));
        return dbInstanceList;
    }

    private DBInstance mkDBInstance(String dbName) {
        return new DBInstance().withDBInstanceIdentifier(dbName)
                .withDBInstanceStatus("running");
    }

}
