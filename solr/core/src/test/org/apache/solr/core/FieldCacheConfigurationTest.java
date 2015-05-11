package org.apache.solr.core;

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

import org.apache.lucene.uninverting.FieldCache;
import org.apache.lucene.uninverting.FieldCacheImpl;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

/**
 * Tests specifying a custom FieldCache implementation
 */
public class FieldCacheConfigurationTest extends SolrTestCaseJ4 {

  public static class MyFieldCache extends FieldCacheImpl {}
  
  private class MyCoreContainer extends CoreContainer {
    FieldCache fieldCacheSet = null;
    
    public MyCoreContainer(NodeConfig config) {
      super(config);
    }
    
    @Override
    protected void setFieldCache(final FieldCache fc) {
      fieldCacheSet = fc;
    }
  }
  
  @Test
  public void testSetFieldCacheThroughSolrXML() throws Exception {
    // Basically doing what CoreContainer.createAndLoad does - to "the real FieldCache" in FieldCache.DEFAULT
    SolrResourceLoader loader = new SolrResourceLoader(TEST_HOME());
    MyCoreContainer cc = new MyCoreContainer(SolrXmlConfig.fromFile(loader, new File(TEST_HOME(), "solr-fieldcache.xml"))); 
    cc.load();
    cc.shutdown();
    
    assertEquals(MyFieldCache.class, cc.fieldCacheSet.getClass());
  }
  
}
