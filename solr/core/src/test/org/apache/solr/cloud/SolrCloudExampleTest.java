package org.apache.solr.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.FilenameFilter;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrCLI;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emulates bin/solr -e cloud -noprompt; bin/post -c gettingstarted example/exampledocs/*.xml;
 * this test is useful for catching regressions in indexing the example docs in collections that
 * use data-driven schema and managed schema features provided by configsets/data_driven_schema_configs.
 */
public class SolrCloudExampleTest extends AbstractFullDistribZkTestBase {

  protected static final transient Logger log = LoggerFactory.getLogger(SolrCloudExampleTest.class);

  public SolrCloudExampleTest() {
    super();
    sliceCount = 2;
  }

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
  }

  @Test
  public void testLoadDocsIntoGettingStartedCollection() throws Exception {
    waitForThingsToLevelOut(30000);

    log.info("testLoadDocsIntoGettingStartedCollection initialized OK ... running test logic");

    String testCollectionName = "gettingstarted";
    File data_driven_schema_configs = new File(ExternalPaths.SCHEMALESS_CONFIGSET);
    assertTrue(data_driven_schema_configs.getAbsolutePath()+" not found!", data_driven_schema_configs.isDirectory());

    Set<String> liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes();
    if (liveNodes.isEmpty())
      fail("No live nodes found! Cannot create a collection until there is at least 1 live node in the cluster.");
    String firstLiveNode = liveNodes.iterator().next();
    String solrUrl = cloudClient.getZkStateReader().getBaseUrlForNodeName(firstLiveNode);

    // create the gettingstarted collection just like the bin/solr script would do
    String[] args = new String[] {
        "create_collection",
        "-name", testCollectionName,
        "-shards", "2",
        "-replicationFactor", "2",
        "-confname", testCollectionName,
        "-confdir", "data_driven_schema_configs",
        "-configsetsDir", data_driven_schema_configs.getParentFile().getParentFile().getAbsolutePath(),
        "-solrUrl", solrUrl
    };
    SolrCLI.CreateCollectionTool tool = new SolrCLI.CreateCollectionTool();
    CommandLine cli = SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args);
    log.info("Creating the '"+testCollectionName+"' collection using SolrCLI with: "+solrUrl);
    tool.runTool(cli);
    assertTrue("Collection '" + testCollectionName + "' doesn't exist after trying to create it!",
        cloudClient.getZkStateReader().getClusterState().hasCollection(testCollectionName));

    // verify the collection is usable ...
    ensureAllReplicasAreActive(testCollectionName, "shard1", 2, 2, 20);
    ensureAllReplicasAreActive(testCollectionName, "shard2", 2, 2, 10);
    cloudClient.setDefaultCollection(testCollectionName);

    // now index docs like bin/post would do but we can't use SimplePostTool because it uses System.exit when
    // it encounters an error, which JUnit doesn't like ...
    log.info("Created collection, now posting example docs!");
    File exampleDocsDir = new File(ExternalPaths.SOURCE_HOME, "example/exampledocs");
    assertTrue(exampleDocsDir.getAbsolutePath()+" not found!", exampleDocsDir.isDirectory());

    File[] xmlFiles = exampleDocsDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".xml");
      }
    });

    // if you add/remove example XML docs, you'll have to fix these expected values
    int expectedXmlFileCount = 14;
    int expectedXmlDocCount = 32;

    assertTrue("Expected 14 example XML files in "+exampleDocsDir.getAbsolutePath(),
        xmlFiles.length == expectedXmlFileCount);

    for (File xml : xmlFiles) {
      ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update");
      req.addFile(xml, "application/xml");
      log.info("POSTing "+xml.getAbsolutePath());
      cloudClient.request(req);
    }
    cloudClient.commit();
    Thread.sleep(1000);

    QueryResponse qr = cloudClient.query(new SolrQuery("*:*"));
    int numFound = (int)qr.getResults().getNumFound();
    assertTrue("Expected "+expectedXmlDocCount+" docs but *:* found "+numFound, numFound == expectedXmlDocCount);

    log.info("testLoadDocsIntoGettingStartedCollection succeeded ... shutting down now!");
  }
}
