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

import org.apache.lucene.uninverting.FieldCache;
import org.apache.solr.common.SolrException;
import org.apache.solr.logging.LogWatcherConfig;
import org.apache.solr.update.UpdateShardHandlerConfig;

import java.util.Properties;


public class NodeConfig {

  private final String nodeName;

  private final String coreRootDirectory;

  private final String configSetBaseDirectory;

  private final String sharedLibDirectory;

  private final PluginInfo shardHandlerFactoryConfig;

  private final UpdateShardHandlerConfig updateShardHandlerConfig;

  private final String coreAdminHandlerClass;

  private final String collectionsAdminHandlerClass;

  private final String infoHandlerClass;

  private final LogWatcherConfig logWatcherConfig;

  private final CloudConfig cloudConfig;

  private final int coreLoadThreads;

  private final int transientCacheSize;
  
  private final boolean useSchemaCache;

  private final String managementPath;
  
  private final PluginInfo recentlyLookedUpOrUpdatedDocumentsCachePluginInfo;
  
  private final FieldCache fieldCache;

  private NodeConfig(String nodeName, String coreRootDirectory, String configSetBaseDirectory, String sharedLibDirectory,
                     PluginInfo shardHandlerFactoryConfig, UpdateShardHandlerConfig updateShardHandlerConfig,
                     String coreAdminHandlerClass, String collectionsAdminHandlerClass, String infoHandlerClass,
                     LogWatcherConfig logWatcherConfig, CloudConfig cloudConfig, int coreLoadThreads,
                     int transientCacheSize, boolean useSchemaCache, String managementPath,
                     PluginInfo recentlyLookedUpOrUpdatedDocumentsCachePluginInfo, FieldCache fieldCache,
                     SolrResourceLoader loader, Properties solrProperties) {
    this.nodeName = nodeName;
    this.coreRootDirectory = coreRootDirectory;
    this.configSetBaseDirectory = configSetBaseDirectory;
    this.sharedLibDirectory = sharedLibDirectory;
    this.shardHandlerFactoryConfig = shardHandlerFactoryConfig;
    this.updateShardHandlerConfig = updateShardHandlerConfig;
    this.coreAdminHandlerClass = coreAdminHandlerClass;
    this.collectionsAdminHandlerClass = collectionsAdminHandlerClass;
    this.infoHandlerClass = infoHandlerClass;
    this.logWatcherConfig = logWatcherConfig;
    this.cloudConfig = cloudConfig;
    this.coreLoadThreads = coreLoadThreads;
    this.transientCacheSize = transientCacheSize;
    this.useSchemaCache = useSchemaCache;
    this.managementPath = managementPath;
    this.recentlyLookedUpOrUpdatedDocumentsCachePluginInfo = recentlyLookedUpOrUpdatedDocumentsCachePluginInfo;
    this.fieldCache = fieldCache;
    this.loader = loader;
    this.solrProperties = solrProperties;

    if (this.cloudConfig != null && this.coreLoadThreads < 2) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "SolrCloud requires a value of at least 2 for coreLoadThreads (configured value = " + this.coreLoadThreads + ")");
    }
  }

  public String getNodeName() {
    return nodeName;
  }

  public String getCoreRootDirectory() {
    return coreRootDirectory;
  }

  public PluginInfo getShardHandlerFactoryPluginInfo() {
    return shardHandlerFactoryConfig;
  }

  public UpdateShardHandlerConfig getUpdateShardHandlerConfig() {
    return updateShardHandlerConfig;
  }

  @Deprecated
  public int getDistributedConnectionTimeout() {
    return updateShardHandlerConfig.getDistributedConnectionTimeout();
  }

  @Deprecated
  public int getDistributedSocketTimeout() {
    return updateShardHandlerConfig.getDistributedSocketTimeout();
  }

  @Deprecated
  public int getMaxUpdateConnections() {
    return updateShardHandlerConfig.getMaxUpdateConnections();
  }

  @Deprecated
  public int getMaxUpdateConnectionsPerHost() {
    return updateShardHandlerConfig.getMaxUpdateConnectionsPerHost();
  }

  public int getCoreLoadThreadCount() {
    return coreLoadThreads;
  }

  public String getSharedLibDirectory() {
    return sharedLibDirectory;
  }

  public String getCoreAdminHandlerClass() {
    return coreAdminHandlerClass;
  }
  
  public String getCollectionsHandlerClass() {
    return collectionsAdminHandlerClass;
  }

  public String getInfoHandlerClass() {
    return infoHandlerClass;
  }

  public boolean hasSchemaCache() {
    return useSchemaCache;
  }

  public String getManagementPath() {
    return managementPath;
  }

  public String getConfigSetBaseDirectory() {
    return configSetBaseDirectory;
  }

  public LogWatcherConfig getLogWatcherConfig() {
    return logWatcherConfig;
  }

  public CloudConfig getCloudConfig() {
    return cloudConfig;
  }

  public int getTransientCacheSize() {
    return transientCacheSize;
  }
  
  public PluginInfo getRecentlyLookedUpOrUpdatedDocumentsCachePluginInfo() {
    return recentlyLookedUpOrUpdatedDocumentsCachePluginInfo;
  }
  
  public FieldCache getFieldCache() {
    return fieldCache;
  }

  protected final SolrResourceLoader loader;
  protected final Properties solrProperties;

  public Properties getSolrProperties() {
    return solrProperties;
  }

  public SolrResourceLoader getSolrResourceLoader() {
    return loader;
  }
  
  public static class NodeConfigBuilder {

    private String coreRootDirectory = "";
    private String configSetBaseDirectory = "configsets";
    private String sharedLibDirectory = "lib";
    private PluginInfo shardHandlerFactoryConfig;
    private UpdateShardHandlerConfig updateShardHandlerConfig = UpdateShardHandlerConfig.DEFAULT;
    private String coreAdminHandlerClass = DEFAULT_ADMINHANDLERCLASS;
    private String collectionsAdminHandlerClass = DEFAULT_COLLECTIONSHANDLERCLASS;
    private String infoHandlerClass = DEFAULT_INFOHANDLERCLASS;
    private LogWatcherConfig logWatcherConfig = new LogWatcherConfig(true, null, null, 50);
    private CloudConfig cloudConfig;
    private int coreLoadThreads = DEFAULT_CORE_LOAD_THREADS;
    private int transientCacheSize = DEFAULT_TRANSIENT_CACHE_SIZE;
    private boolean useSchemaCache = false;
    private String managementPath;
    private PluginInfo recentlyLookedUpOrUpdatedDocumentsCachePluginInfo;
    private FieldCache fieldCache;
    private Properties solrProperties = new Properties();

    private final SolrResourceLoader loader;
    private final String nodeName;

    private static final int DEFAULT_CORE_LOAD_THREADS = 3;

    private static final int DEFAULT_TRANSIENT_CACHE_SIZE = Integer.MAX_VALUE;

    private static final String DEFAULT_ADMINHANDLERCLASS = "org.apache.solr.handler.admin.CoreAdminHandler";
    private static final String DEFAULT_INFOHANDLERCLASS = "org.apache.solr.handler.admin.InfoHandler";
    private static final String DEFAULT_COLLECTIONSHANDLERCLASS = "org.apache.solr.handler.admin.CollectionsHandler";

    public NodeConfigBuilder(String nodeName, SolrResourceLoader loader) {
      this.nodeName = nodeName;
      this.loader = loader;
      this.coreRootDirectory = loader.getInstanceDir();
    }

    public NodeConfigBuilder setCoreRootDirectory(String coreRootDirectory) {
      this.coreRootDirectory = loader.resolve(coreRootDirectory);
      return this;
    }

    public NodeConfigBuilder setConfigSetBaseDirectory(String configSetBaseDirectory) {
      this.configSetBaseDirectory = configSetBaseDirectory;
      return this;
    }

    public NodeConfigBuilder setSharedLibDirectory(String sharedLibDirectory) {
      this.sharedLibDirectory = sharedLibDirectory;
      return this;
    }

    public NodeConfigBuilder setShardHandlerFactoryConfig(PluginInfo shardHandlerFactoryConfig) {
      this.shardHandlerFactoryConfig = shardHandlerFactoryConfig;
      return this;
    }

    public NodeConfigBuilder setUpdateShardHandlerConfig(UpdateShardHandlerConfig updateShardHandlerConfig) {
      this.updateShardHandlerConfig = updateShardHandlerConfig;
      return this;
    }

    public NodeConfigBuilder setCoreAdminHandlerClass(String coreAdminHandlerClass) {
      this.coreAdminHandlerClass = coreAdminHandlerClass;
      return this;
    }

    public NodeConfigBuilder setCollectionsAdminHandlerClass(String collectionsAdminHandlerClass) {
      this.collectionsAdminHandlerClass = collectionsAdminHandlerClass;
      return this;
    }

    public NodeConfigBuilder setInfoHandlerClass(String infoHandlerClass) {
      this.infoHandlerClass = infoHandlerClass;
      return this;
    }

    public NodeConfigBuilder setLogWatcherConfig(LogWatcherConfig logWatcherConfig) {
      this.logWatcherConfig = logWatcherConfig;
      return this;
    }

    public NodeConfigBuilder setCloudConfig(CloudConfig cloudConfig) {
      this.cloudConfig = cloudConfig;
      return this;
    }

    public NodeConfigBuilder setCoreLoadThreads(int coreLoadThreads) {
      this.coreLoadThreads = coreLoadThreads;
      return this;
    }

    public NodeConfigBuilder setTransientCacheSize(int transientCacheSize) {
      this.transientCacheSize = transientCacheSize;
      return this;
    }

    public NodeConfigBuilder setUseSchemaCache(boolean useSchemaCache) {
      this.useSchemaCache = useSchemaCache;
      return this;
    }

    public NodeConfigBuilder setManagementPath(String managementPath) {
      this.managementPath = managementPath;
      return this;
    }
    
    public NodeConfigBuilder setRecentlyLookedUpOrUpdatedDocumentsCachePluginInfo(PluginInfo recentlyLookedUpOrUpdatedDocumentsCachePluginInfo) {
      this.recentlyLookedUpOrUpdatedDocumentsCachePluginInfo = recentlyLookedUpOrUpdatedDocumentsCachePluginInfo;
      return this;
    }
    
    public NodeConfigBuilder setFieldCache(FieldCache fieldCache) {
      this.fieldCache = fieldCache;
      return this;
    }

    public NodeConfigBuilder setSolrProperties(Properties solrProperties) {
      this.solrProperties = solrProperties;
      return this;
    }

    public NodeConfig build() {
      return new NodeConfig(nodeName, coreRootDirectory, configSetBaseDirectory, sharedLibDirectory, shardHandlerFactoryConfig,
                            updateShardHandlerConfig, coreAdminHandlerClass, collectionsAdminHandlerClass, infoHandlerClass,
                            logWatcherConfig, cloudConfig, coreLoadThreads, transientCacheSize, useSchemaCache, managementPath,
                            recentlyLookedUpOrUpdatedDocumentsCachePluginInfo, fieldCache, loader, solrProperties);
    }
  }
}

