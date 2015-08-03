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

package org.apache.solr.client.solrj.io;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.security.AuthCredentials;

public abstract class TupleStream implements Serializable {

  private static final long serialVersionUID = 1;
  
  private static boolean DEFAULT_PREEMPTIVE_AUTH = true;
  private static Function<String, HttpSolrClient> DEFAULT_SOLR_CLIENT_FACTORY = baseUrl -> new HttpSolrClient(baseUrl);
  
  private TupleStream parent;
  private transient Optional<AuthCredentials> authCredentials;
  private transient Optional<Boolean> preemptiveAuthentication;
  private transient Function<String, HttpSolrClient> overriddenSolrClientFactory;

  public TupleStream() {

  }
  
  public TupleStream getParent() {
    return parent;
  }
  
  public void setParent(TupleStream parent) {
    this.parent = parent;
  }
  
  public Function<String, HttpSolrClient> getSolrClientFactory() {
    Function<String, HttpSolrClient> factory = getOverriddenSolrClientFactory();
    return (factory != null)?factory:DEFAULT_SOLR_CLIENT_FACTORY;
  }
  
  public void overrideSolrClientFactory(Function<String, HttpSolrClient> solrClientConstructor) {
    this.overriddenSolrClientFactory = solrClientConstructor;
  }
  
  private Function<String, HttpSolrClient> getOverriddenSolrClientFactory() {
    return (overriddenSolrClientFactory != null)?
        overriddenSolrClientFactory:
        (parent != null)?parent.getOverriddenSolrClientFactory():null;
  }
  
  /** As in SolrRequest and SolrQueryRequest
   * - null means that no one has yet considered what it should be
   * - absent means that it has explicitly been decided not to use any credentials
   * - non-null and not absent means that it has explicitly been decided to use the present credentials
   */
  public Optional<AuthCredentials> getAuthCredentials() {
    if (authCredentials != null || parent == null) return authCredentials;
    return parent.getAuthCredentials();
  }

  public void setAuthCredentials(AuthCredentials authCredentials) {
    setAuthCredentials(Optional.ofNullable(authCredentials));
  }

  public void setAuthCredentials(Optional<AuthCredentials> authCredentials) {
    this.authCredentials = authCredentials;
  }

  public boolean getPreemptiveAuthentication() {
    return (preemptiveAuthentication != null && preemptiveAuthentication.isPresent())?
        preemptiveAuthentication.get():
        ((parent != null)?parent.getPreemptiveAuthentication():DEFAULT_PREEMPTIVE_AUTH);
  }

  public void setPreemptiveAuthentication(boolean preemptiveAuthentication) {
    this.preemptiveAuthentication = Optional.of(preemptiveAuthentication);
  }

  public abstract void setStreamContext(StreamContext context);

  public abstract List<TupleStream> children();

  public abstract void open() throws IOException;

  public abstract void close() throws IOException;

  public abstract Tuple read() throws IOException;

  public int getCost() {
    return 0;
  }
}