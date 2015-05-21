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

package org.apache.solr.client.solrj;

import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.StreamingBinaryResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.security.AuthCredentials;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Abstraction through which all communication with a Solr server may be routed
 *
 * @since 5.0, replaced {@code SolrServer}
 */
public abstract class SolrClient implements Serializable, Closeable {

  private static final long serialVersionUID = 1L;

  private DocumentObjectBinder binder;

  /**
   * Adds a collection of documents
   *
   * @param collection the Solr collection to add documents to
   * @param docs  the collection of documents
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @since Solr 5.1
   */
  public UpdateResponse add(String collection, Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
    return add(collection, docs, -1);
  }

  /**
   * Adds a collection of documents
   *
   * @param docs  the collection of documents
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
    return add(null, docs);
  }

  /**
   * Adds a collection of documents, specifying max time before they become committed
   *
   * @param collection the Solr collection to add documents to
   * @param docs  the collection of documents
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @since Solr 5.1
   */
  public UpdateResponse add(String collection, Collection<SolrInputDocument> docs, int commitWithinMs) throws SolrServerException, IOException {
    return add(collection, docs, commitWithinMs, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse add(String collection, Collection<SolrInputDocument> docs, int commitWithinMs, AuthCredentials authCredentials) throws SolrServerException, IOException {
    return add(collection, docs, commitWithinMs, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse add(String collection, Collection<SolrInputDocument> docs, int commitWithinMs, Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(docs);
    req.setCommitWithin(commitWithinMs);
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Adds a collection of documents, specifying max time before they become committed
   *
   * @param docs  the collection of documents
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @since Solr 3.5
   */
  public UpdateResponse add(Collection<SolrInputDocument> docs, int commitWithinMs) throws SolrServerException, IOException {
    return add(null, docs, commitWithinMs);
  }

  /**
   * Adds a single document
   *
   * @param collection the Solr collection to add the document to
   * @param doc  the input document
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse add(String collection, SolrInputDocument doc) throws SolrServerException, IOException {
    return add(collection, doc, -1);
  }

  /**
   * Adds a single document
   *
   * @param doc  the input document
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse add(SolrInputDocument doc) throws SolrServerException, IOException {
    return add(null, doc);
  }

  /**
   * Adds a single document specifying max time before it becomes committed
   *
   * @param collection the Solr collection to add the document to
   * @param doc  the input document
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @since solr 5.1
   */
  public UpdateResponse add(String collection, SolrInputDocument doc, int commitWithinMs) throws SolrServerException, IOException {
    return add(collection, doc, commitWithinMs, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse add(String collection, SolrInputDocument doc, int commitWithinMs, AuthCredentials authCredentials) throws SolrServerException, IOException {
    return add(collection, doc, commitWithinMs, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse add(String collection, SolrInputDocument doc, int commitWithinMs, Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.setCommitWithin(commitWithinMs);
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Adds a single document specifying max time before it becomes committed
   *
   * @param doc  the input document
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @since solr 3.5
   */
  public UpdateResponse add(SolrInputDocument doc, int commitWithinMs) throws SolrServerException, IOException {
    return add(doc, commitWithinMs, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse add(SolrInputDocument doc, int commitWithinMs, AuthCredentials authCredentials) throws SolrServerException, IOException {
    return add(doc, commitWithinMs, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse add(SolrInputDocument doc, int commitWithinMs, Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    return add(null, doc, commitWithinMs, authCredentials);
  }

  /**
   * Adds the documents supplied by the given iterator.
   *
   * @param collection the Solr collection to add the documents to
   * @param docIterator
   *          the iterator which returns SolrInputDocument instances
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse add(String collection, Iterator<SolrInputDocument> docIterator)
      throws SolrServerException, IOException {
    return add(collection, docIterator, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse add(String collection, Iterator<SolrInputDocument> docIterator, AuthCredentials authCredentials)
      throws SolrServerException, IOException {
    return add(collection, docIterator, Optional.ofNullable(authCredentials));
  }  
  
  private UpdateResponse add(String collection, Iterator<SolrInputDocument> docIterator, Optional<AuthCredentials> authCredentials)
      throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.setDocIterator(docIterator);
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Adds the documents supplied by the given iterator.
   *
   * @param docIterator
   *          the iterator which returns SolrInputDocument instances
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse add(Iterator<SolrInputDocument> docIterator) throws SolrServerException, IOException {
    return add(null, docIterator);
  }

  /**
   * Adds a single bean
   *
   * The bean is converted to a {@link SolrInputDocument} by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param collection to Solr collection to add documents to
   * @param obj  the input bean
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBean(String collection, Object obj) throws IOException, SolrServerException {
    return addBean(collection, obj, -1);
  }

  /**
   * Adds a single bean
   *
   * The bean is converted to a {@link SolrInputDocument} by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param obj  the input bean
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBean(Object obj) throws IOException, SolrServerException {
    return addBean(null, obj, -1);
  }

  /**
   * Adds a single bean specifying max time before it becomes committed
   *
   * The bean is converted to a {@link SolrInputDocument} by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param collection to Solr collection to add documents to
   * @param obj  the input bean
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBean(String collection, Object obj, int commitWithinMs) throws IOException, SolrServerException {
    return addBean(collection, obj, commitWithinMs, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse addBean(String collection, Object obj, int commitWithinMs, AuthCredentials authCredentials) throws IOException, SolrServerException {
    return addBean(collection, obj, commitWithinMs, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse addBean(String collection, Object obj, int commitWithinMs, Optional<AuthCredentials> authCredentials) throws IOException, SolrServerException {
    return add(collection, getBinder().toSolrInputDocument(obj), commitWithinMs, authCredentials);
  }

  /**
   * Adds a single bean specifying max time before it becomes committed
   *
   * The bean is converted to a {@link SolrInputDocument} by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param obj  the input bean
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBean(Object obj, int commitWithinMs) throws IOException, SolrServerException {
    return addBean(obj, commitWithinMs, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse addBean(Object obj, int commitWithinMs, AuthCredentials authCredentials) throws IOException, SolrServerException {
    return addBean(obj, commitWithinMs, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse addBean(Object obj, int commitWithinMs, Optional<AuthCredentials> authCredentials) throws IOException, SolrServerException {
    return add(null, getBinder().toSolrInputDocument(obj), commitWithinMs, authCredentials);
  }

  /**
   * Adds a collection of beans
   *
   * The beans are converted to {@link SolrInputDocument}s by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param collection the Solr collection to add documents to
   * @param beans  the collection of beans
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBeans(String collection, Collection<?> beans) throws SolrServerException, IOException {
    return addBeans(collection, beans, -1);
  }

  /**
   * Adds a collection of beans
   *
   * The beans are converted to {@link SolrInputDocument}s by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param beans  the collection of beans
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBeans(Collection<?> beans) throws SolrServerException, IOException {
    return addBeans(null, beans, -1);
  }

  /**
   * Adds a collection of beans specifying max time before they become committed
   *
   * The beans are converted to {@link SolrInputDocument}s by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param collection the Solr collection to add documents to
   * @param beans  the collection of beans
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @see SolrClient#getBinder()
   *
   * @since solr 5.1
   */
  public UpdateResponse addBeans(String collection, Collection<?> beans, int commitWithinMs) throws SolrServerException, IOException {
    return addBeans(collection, beans, commitWithinMs, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse addBeans(String collection, Collection<?> beans, int commitWithinMs, AuthCredentials authCredentials) throws SolrServerException, IOException {
    return addBeans(collection, beans, commitWithinMs, Optional.ofNullable(authCredentials));
  }
    
  private UpdateResponse addBeans(String collection, Collection<?> beans, int commitWithinMs, Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {  
    DocumentObjectBinder binder = this.getBinder();
    ArrayList<SolrInputDocument> docs =  new ArrayList<>(beans.size());
    for (Object bean : beans) {
      docs.add(binder.toSolrInputDocument(bean));
    }
    return add(collection, docs, commitWithinMs, authCredentials);
  }

  /**
   * Adds a collection of beans specifying max time before they become committed
   *
   * The beans are converted to {@link SolrInputDocument}s by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param beans  the collection of beans
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @see SolrClient#getBinder()
   *
   * @since solr 3.5
   */
  public UpdateResponse addBeans(Collection<?> beans, int commitWithinMs) throws SolrServerException, IOException {
    return addBeans(null, beans, commitWithinMs);
  }

  /**
   * Adds the beans supplied by the given iterator.
   *
   * @param collection the Solr collection to add the documents to
   * @param beanIterator
   *          the iterator which returns Beans
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBeans(String collection, final Iterator<?> beanIterator)
      throws SolrServerException, IOException {
    return addBeans(collection, beanIterator, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse addBeans(String collection, final Iterator<?> beanIterator, AuthCredentials authCredentials)
      throws SolrServerException, IOException {
    return addBeans(collection, beanIterator, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse addBeans(String collection, final Iterator<?> beanIterator, Optional<AuthCredentials> authCredentials)
      throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.setDocIterator(new Iterator<SolrInputDocument>() {

      @Override
      public boolean hasNext() {
        return beanIterator.hasNext();
      }

      @Override
      public SolrInputDocument next() {
        Object o = beanIterator.next();
        if (o == null) return null;
        return getBinder().toSolrInputDocument(o);
      }

      @Override
      public void remove() {
        beanIterator.remove();
      }
    });
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Adds the beans supplied by the given iterator.
   *
   * @param beanIterator
   *          the iterator which returns Beans
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBeans(final Iterator<?> beanIterator) throws SolrServerException, IOException {
    return addBeans(null, beanIterator);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   *
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   *
   * @param collection the Solr collection to send the commit to
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse commit(String collection) throws SolrServerException, IOException {
    return commit(collection, true, true);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   *
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse commit() throws SolrServerException, IOException {
    return commit((Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse commit(AuthCredentials authCredentials) throws SolrServerException, IOException {
    return commit(Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse commit(Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    return commit(null, true, true, authCredentials);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   *
   * @param collection the Solr collection to send the commit to
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the
   *                      main query searcher, making the changes visible
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher)
      throws SolrServerException, IOException {
    return commit(collection, waitFlush, waitSearcher, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher, AuthCredentials authCredentials)
      throws SolrServerException, IOException {
    return commit(collection, waitFlush, waitSearcher, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher, Optional<AuthCredentials> authCredentials)
      throws SolrServerException, IOException {
    UpdateRequest req = (UpdateRequest)new UpdateRequest()
        .setAction(UpdateRequest.ACTION.COMMIT, waitFlush, waitSearcher);
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   *
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the
   *                      main query searcher, making the changes visible
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    return commit(null, waitFlush, waitSearcher);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   *
   * @param collection the Solr collection to send the commit to
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the
   *                      main query searcher, making the changes visible
   * @param softCommit makes index changes visible while neither fsync-ing index files
   *                   nor writing a new index descriptor
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher, boolean softCommit)
      throws SolrServerException, IOException {
    return commit(collection, waitFlush, waitSearcher, softCommit, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher, boolean softCommit, AuthCredentials authCredentials)
      throws SolrServerException, IOException {
    return commit(collection, waitFlush, waitSearcher, softCommit, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher, boolean softCommit, Optional<AuthCredentials> authCredentials)
      throws SolrServerException, IOException {
    UpdateRequest req = (UpdateRequest)new UpdateRequest()
        .setAction(UpdateRequest.ACTION.COMMIT, waitFlush, waitSearcher, softCommit);
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   *
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the
   *                      main query searcher, making the changes visible
   * @param softCommit makes index changes visible while neither fsync-ing index files
   *                   nor writing a new index descriptor
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse commit(boolean waitFlush, boolean waitSearcher, boolean softCommit)
      throws SolrServerException, IOException {
    return commit(null, waitFlush, waitSearcher, softCommit);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   *
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   *
   * Note: In most cases it is not required to do explicit optimize
   *
   * @param collection the Solr collection to send the optimize to
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse optimize(String collection) throws SolrServerException, IOException {
    return optimize(collection, true, true, 1);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   *
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   *
   * Note: In most cases it is not required to do explicit optimize
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse optimize() throws SolrServerException, IOException {
    return optimize(null, true, true, 1);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   *
   * @param collection the Solr collection to send the optimize to
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as
   *                      the main query searcher, making the changes visible
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse optimize(String collection, boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    return optimize(collection, waitFlush, waitSearcher, 1);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   *
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as
   *                      the main query searcher, making the changes visible
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    return optimize(null, waitFlush, waitSearcher);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   *
   * Note: In most cases it is not required to do explicit optimize
   *
   * @param collection the Solr collection to send the optimize to
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as
   *                      the main query searcher, making the changes visible
   * @param maxSegments  optimizes down to at most this number of segments
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse optimize(String collection, boolean waitFlush, boolean waitSearcher, int maxSegments)
      throws SolrServerException, IOException {
    return optimize(collection, waitFlush, waitSearcher, maxSegments, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse optimize(String collection, boolean waitFlush, boolean waitSearcher, int maxSegments, AuthCredentials authCredentials)
      throws SolrServerException, IOException {
    return optimize(collection, waitFlush, waitSearcher, maxSegments, Optional.ofNullable(authCredentials));
  }
    
  private UpdateResponse optimize(String collection, boolean waitFlush, boolean waitSearcher, int maxSegments, Optional<AuthCredentials> authCredentials)
      throws SolrServerException, IOException {  
    UpdateRequest req = (UpdateRequest)new UpdateRequest()
        .setAction(UpdateRequest.ACTION.OPTIMIZE, waitFlush, waitSearcher, maxSegments);
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   *
   * Note: In most cases it is not required to do explicit optimize
   *
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as
   *                      the main query searcher, making the changes visible
   * @param maxSegments  optimizes down to at most this number of segments
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments)
      throws SolrServerException, IOException {
    return optimize(null, waitFlush, waitSearcher, maxSegments);
  }

  /**
   * Performs a rollback of all non-committed documents pending.
   *
   * Note that this is not a true rollback as in databases. Content you have previously
   * added may have been committed due to autoCommit, buffer full, other client performing
   * a commit etc.
   *
   * @param collection the Solr collection to send the rollback to
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse rollback(String collection) throws SolrServerException, IOException {
    return rollback(collection, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse rollback(String collection, AuthCredentials authCredentials) throws SolrServerException, IOException {
    return rollback(collection, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse rollback(String collection, Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    UpdateRequest req = (UpdateRequest)new UpdateRequest().rollback();
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Performs a rollback of all non-committed documents pending.
   *
   * Note that this is not a true rollback as in databases. Content you have previously
   * added may have been committed due to autoCommit, buffer full, other client performing
   * a commit etc.
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse rollback() throws SolrServerException, IOException {
    return rollback(null);
  }

  /**
   * Deletes a single document by unique ID
   *
   * @param collection the Solr collection to delete the document from
   * @param id  the ID of the document to delete
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse deleteById(String collection, String id) throws SolrServerException, IOException {
    return deleteById(collection, id, -1);
  }

  /**
   * Deletes a single document by unique ID
   *
   * @param id  the ID of the document to delete
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse deleteById(String id) throws SolrServerException, IOException {
    return deleteById(null, id);
  }

  /**
   * Deletes a single document by unique ID, specifying max time before commit
   *
   * @param collection the Solr collection to delete the document from
   * @param id  the ID of the document to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since 5.1
   */
  public UpdateResponse deleteById(String collection, String id, int commitWithinMs) throws SolrServerException, IOException {
    return deleteById(collection, id, commitWithinMs, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse deleteById(String collection, String id, int commitWithinMs, AuthCredentials authCredentials) throws SolrServerException, IOException {
    return deleteById(collection, id, commitWithinMs, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse deleteById(String collection, String id, int commitWithinMs, Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.deleteById(id);
    req.setCommitWithin(commitWithinMs);
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Deletes a single document by unique ID, specifying max time before commit
   *
   * @param id  the ID of the document to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since 3.6
   */
  public UpdateResponse deleteById(String id, int commitWithinMs) throws SolrServerException, IOException {
    return deleteById(null, id, commitWithinMs);
  }

  /**
   * Deletes a list of documents by unique ID
   *
   * @param collection the Solr collection to delete the documents from
   * @param ids  the list of document IDs to delete
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse deleteById(String collection, List<String> ids) throws SolrServerException, IOException {
    return deleteById(collection, ids, -1);
  }

  /**
   * Deletes a list of documents by unique ID
   *
   * @param ids  the list of document IDs to delete
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse deleteById(List<String> ids) throws SolrServerException, IOException {
    return deleteById(null, ids);
  }

  /**
   * Deletes a list of documents by unique ID, specifying max time before commit
   *
   * @param collection the Solr collection to delete the documents from
   * @param ids  the list of document IDs to delete 
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since 5.1
   */
  public UpdateResponse deleteById(String collection, List<String> ids, int commitWithinMs) throws SolrServerException, IOException {
    return deleteById(collection, ids, commitWithinMs, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse deleteById(String collection, List<String> ids, int commitWithinMs, AuthCredentials authCredentials) throws SolrServerException, IOException {
    return deleteById(collection, ids, commitWithinMs, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse deleteById(String collection, List<String> ids, int commitWithinMs, Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.deleteById(ids);
    req.setCommitWithin(commitWithinMs);
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Deletes a list of documents by unique ID, specifying max time before commit
   *
   * @param ids  the list of document IDs to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since 3.6
   */
  public UpdateResponse deleteById(List<String> ids, int commitWithinMs) throws SolrServerException, IOException {
    return deleteById(null, ids, commitWithinMs);
  }

  /**
   * Deletes documents from the index based on a query
   *
   * @param collection the Solr collection to delete the documents from
   * @param query  the query expressing what documents to delete
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse deleteByQuery(String collection, String query) throws SolrServerException, IOException {
    return deleteByQuery(collection, query, -1);
  }

  /**
   * Deletes documents from the index based on a query
   *
   * @param query  the query expressing what documents to delete
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse deleteByQuery(String query) throws SolrServerException, IOException {
    return deleteByQuery(null, query);
  }

  /**
   * Deletes documents from the index based on a query, specifying max time before commit
   *
   * @param collection the Solr collection to delete the documents from
   * @param query  the query expressing what documents to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since 5.1
   */
  public UpdateResponse deleteByQuery(String collection, String query, int commitWithinMs) throws SolrServerException, IOException {
    return deleteByQuery(collection, query, commitWithinMs, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse deleteByQuery(String collection, String query, int commitWithinMs, AuthCredentials authCredentials) throws SolrServerException, IOException {
    return deleteByQuery(collection, query, commitWithinMs, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse deleteByQuery(String collection, String query, int commitWithinMs, Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.deleteByQuery(query);
    req.setCommitWithin(commitWithinMs);
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Deletes documents from the index based on a query, specifying max time before commit
   *
   * @param query  the query expressing what documents to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since 3.6
   */
  public UpdateResponse deleteByQuery(String query, int commitWithinMs) throws SolrServerException, IOException {
    return deleteByQuery(query, commitWithinMs, (Optional<AuthCredentials>)null);
  }
  
  public UpdateResponse deleteByQuery(String query, int commitWithinMs, AuthCredentials authCredentials) throws SolrServerException, IOException {
    return deleteByQuery(query, commitWithinMs, Optional.ofNullable(authCredentials));
  }
  
  private UpdateResponse deleteByQuery(String query, int commitWithinMs, Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    return deleteByQuery(null, query, commitWithinMs, authCredentials);
  }

  /**
   * Issues a ping request to check if the server is alive
   *
   * @return a {@link org.apache.solr.client.solrj.response.SolrPingResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrPingResponse ping() throws SolrServerException, IOException {
    return ping((Optional<AuthCredentials>)null);
  }
  
  public SolrPingResponse ping(AuthCredentials authCredentials) throws SolrServerException, IOException {
    return ping(Optional.ofNullable(authCredentials));
  }
  
  private SolrPingResponse ping(Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    SolrPing req = new SolrPing();
    req.setAuthCredentials(authCredentials);
    return req.process(this, null);
  }

  /**
   * Performs a query to the Solr server
   *
   * @param collection the Solr collection to query
   * @param params  an object holding all key/value parameters to send along the request
   *
   * @return a {@link org.apache.solr.client.solrj.response.QueryResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public QueryResponse query(String collection, SolrParams params) throws SolrServerException, IOException {
    return query(collection, params, (Optional<AuthCredentials>)null);
  }
  
  public QueryResponse query(String collection, SolrParams params, AuthCredentials authCredentials) throws SolrServerException, IOException {
    return query(collection, params, Optional.ofNullable(authCredentials));
  }
  
  private QueryResponse query(String collection, SolrParams params, Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    QueryRequest req = new QueryRequest(params);
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Performs a query to the Solr server
   *
   * @param params  an object holding all key/value parameters to send along the request
   *
   * @return a {@link org.apache.solr.client.solrj.response.QueryResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public QueryResponse query(SolrParams params) throws SolrServerException, IOException {
    return query(params, (Optional<AuthCredentials>)null);
  }
  
  public QueryResponse query(SolrParams params, AuthCredentials authCredentials) throws SolrServerException, IOException {
    return query(params, Optional.ofNullable(authCredentials));
  }
   
  private QueryResponse query(SolrParams params, Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    return query(null, params, authCredentials);
  }

  /**
   * Performs a query to the Solr server
   *
   * @param collection the Solr collection to query
   * @param params  an object holding all key/value parameters to send along the request
   * @param method  specifies the HTTP method to use for the request, such as GET or POST
   *
   * @return a {@link org.apache.solr.client.solrj.response.QueryResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public QueryResponse query(String collection, SolrParams params, METHOD method) throws SolrServerException, IOException {
    return query(collection, params, method, (Optional<AuthCredentials>)null);
  }
  
  public QueryResponse query(String collection, SolrParams params, METHOD method, AuthCredentials authCredentials) throws SolrServerException, IOException {
    return query(collection, params, method, Optional.ofNullable(authCredentials));
  }
  
  private QueryResponse query(String collection, SolrParams params, METHOD method, Optional<AuthCredentials> authCredentials) throws SolrServerException, IOException {
    QueryRequest req = new QueryRequest(params, method);
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Performs a query to the Solr server
   *
   * @param params  an object holding all key/value parameters to send along the request
   * @param method  specifies the HTTP method to use for the request, such as GET or POST
   *
   * @return a {@link org.apache.solr.client.solrj.response.QueryResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public QueryResponse query(SolrParams params, METHOD method) throws SolrServerException, IOException {
    return query(null, params, method);
  }

  /**
   * Query solr, and stream the results.  Unlike the standard query, this will 
   * send events for each Document rather then add them to the QueryResponse.
   *
   * Although this function returns a 'QueryResponse' it should be used with care
   * since it excludes anything that was passed to callback.  Also note that
   * future version may pass even more info to the callback and may not return 
   * the results in the QueryResponse.
   *
   * @param collection the Solr collection to query
   * @param params  an object holding all key/value parameters to send along the request
   * @param callback the callback to stream results to
   *
   * @return a {@link org.apache.solr.client.solrj.response.QueryResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since solr 5.1
   */
  public QueryResponse queryAndStreamResponse(String collection, SolrParams params, StreamingResponseCallback callback)
      throws SolrServerException, IOException {
    return queryAndStreamResponse(collection, params, callback, (Optional<AuthCredentials>)null);
  }
  
  public QueryResponse queryAndStreamResponse(String collection, SolrParams params, StreamingResponseCallback callback, AuthCredentials authCredentials)
      throws SolrServerException, IOException {
    return queryAndStreamResponse(collection, params, callback, Optional.ofNullable(authCredentials));
  }
  
  private QueryResponse queryAndStreamResponse(String collection, SolrParams params, StreamingResponseCallback callback, Optional<AuthCredentials> authCredentials)
      throws SolrServerException, IOException {
    ResponseParser parser = new StreamingBinaryResponseParser(callback);
    QueryRequest req = new QueryRequest(params);
    req.setStreamingResponseCallback(callback);
    req.setResponseParser(parser);
    req.setAuthCredentials(authCredentials);
    return req.process(this, collection);
  }

  /**
   * Query solr, and stream the results.  Unlike the standard query, this will
   * send events for each Document rather then add them to the QueryResponse.
   *
   * Although this function returns a 'QueryResponse' it should be used with care
   * since it excludes anything that was passed to callback.  Also note that
   * future version may pass even more info to the callback and may not return
   * the results in the QueryResponse.
   *
   * @param params  an object holding all key/value parameters to send along the request
   * @param callback the callback to stream results to
   *
   * @return a {@link org.apache.solr.client.solrj.response.QueryResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since solr 4.0
   */
  public QueryResponse queryAndStreamResponse(SolrParams params, StreamingResponseCallback callback)
      throws SolrServerException, IOException {
    return queryAndStreamResponse(null, params, callback);
  }

  /**
   * Retrieves the SolrDocument associated with the given identifier.
   *
   * @param collection the Solr collection to query
   * @param id the id
   *
   * @return retrieved SolrDocument, or null if no document is found.
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocument getById(String collection, String id) throws SolrServerException, IOException {
    return getById(collection, id, null);
  }
  /**
   * Retrieves the SolrDocument associated with the given identifier.
   *
   * @param id the id
   *
   * @return retrieved SolrDocument, or null if no document is found.
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocument getById(String id) throws SolrServerException, IOException {
    return getById(null, id, null);
  }

  /**
   * Retrieves the SolrDocument associated with the given identifier and uses
   * the SolrParams to execute the request.
   *
   * @param collection the Solr collection to query
   * @param id the id
   * @param params additional parameters to add to the query
   *
   * @return retrieved SolrDocument, or null if no document is found.
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocument getById(String collection, String id, SolrParams params) throws SolrServerException, IOException {
    SolrDocumentList docs = getById(collection, Arrays.asList(id), params);
    if (!docs.isEmpty()) {
      return docs.get(0);
    }
    return null;
  }

  /**
   * Retrieves the SolrDocument associated with the given identifier and uses
   * the SolrParams to execute the request.
   *
   * @param id the id
   * @param params additional parameters to add to the query
   *
   * @return retrieved SolrDocument, or null if no document is found.
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocument getById(String id, SolrParams params) throws SolrServerException, IOException {
    return getById(null, id, params);
  }

  /**
   * Retrieves the SolrDocuments associated with the given identifiers.
   *
   * If a document was not found, it will not be added to the SolrDocumentList.
   *
   * @param collection the Solr collection to query
   * @param ids the ids
   *
   * @return a SolrDocumentList, or null if no documents were found
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocumentList getById(String collection, Collection<String> ids) throws SolrServerException, IOException {
    return getById(collection, ids, null);
  }

  /**
   * Retrieves the SolrDocuments associated with the given identifiers.
   *
   * If a document was not found, it will not be added to the SolrDocumentList.
   *
   * @param ids the ids
   *
   * @return a SolrDocumentList, or null if no documents were found
   *
   *  @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocumentList getById(Collection<String> ids) throws SolrServerException, IOException {
    return getById(null, ids);
  }

  /**
   * Retrieves the SolrDocuments associated with the given identifiers and uses
   * the SolrParams to execute the request.
   *
   * If a document was not found, it will not be added to the SolrDocumentList.
   *
   * @param collection the Solr collection to query
   * @param ids the ids
   * @param params additional parameters to add to the query
   *
   * @return a SolrDocumentList, or null if no documents were found
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocumentList getById(String collection, Collection<String> ids, SolrParams params)
      throws SolrServerException, IOException {
    return getById(collection, ids, params, (Optional<AuthCredentials>)null);
  }
  
  public SolrDocumentList getById(String collection, Collection<String> ids, SolrParams params, AuthCredentials authCredentials)
      throws SolrServerException, IOException {
    return getById(collection, ids, params, Optional.ofNullable(authCredentials));
  }
  
  private SolrDocumentList getById(String collection, Collection<String> ids, SolrParams params, Optional<AuthCredentials> authCredentials)
      throws SolrServerException, IOException {
    if (ids == null || ids.isEmpty()) {
      throw new IllegalArgumentException("Must provide an identifier of a document to retrieve.");
    }

    ModifiableSolrParams reqParams = new ModifiableSolrParams(params);
    if (StringUtils.isEmpty(reqParams.get(CommonParams.QT))) {
      reqParams.set(CommonParams.QT, "/get");
    }
    reqParams.set("ids", (String[]) ids.toArray());

    return query(collection, reqParams, authCredentials).getResults();
  }

  /**
   * Retrieves the SolrDocuments associated with the given identifiers and uses
   * the SolrParams to execute the request.
   *
   * If a document was not found, it will not be added to the SolrDocumentList.
   *
   * @param ids the ids
   * @param params additional parameters to add to the query
   *
   * @return a SolrDocumentList, or null if no documents were found
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocumentList getById(Collection<String> ids, SolrParams params) throws SolrServerException, IOException {
    return getById(null, ids, params);
  }

  /**
   * Execute a request against a Solr server for a given collection
   *
   * @param request the request to execute
   * @param collection the collection to execute the request against
   *
   * @return a {@link NamedList} containing the response from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public final NamedList<Object> request(final SolrRequest request, String collection)
      throws SolrServerException, IOException {
    manipulateRequestBeforeFire(request);
    return doRequest(request, collection);
  }
  
  public void manipulateRequestBeforeFire(final SolrRequest request) {
    // Making sure optional request.getAuthCredentials is not null - either present or absent
    if (request.getAuthCredentials() == null) request.setAuthCredentials(getAuthCredentialsForRequestWhereItHasNotBeenExplicitlyDecided(request));
  }
  
  protected Optional<AuthCredentials> getAuthCredentialsForRequestWhereItHasNotBeenExplicitlyDecided(final SolrRequest request) {
    return null;
  }

  
  public abstract NamedList<Object> doRequest(final SolrRequest request, String collection)
      throws SolrServerException, IOException;

  /**
   * Execute a request against a Solr server
   *
   * @param request the request to execute
   *
   * @return a {@link NamedList} containing the response from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public final NamedList<Object> request(final SolrRequest request) throws SolrServerException, IOException {
    return request(request, null);
  }

  /**
   * Get the {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder} for this client.
   *
   * @return a DocumentObjectBinder
   *
   * @see SolrClient#addBean
   * @see SolrClient#addBeans
   */
  public DocumentObjectBinder getBinder() {
    if(binder == null){
      binder = new DocumentObjectBinder();
    }
    return binder;
  }

  /**
   * Release allocated resources.
   *
   * @since solr 4.0
   * @deprecated Use close() instead.
   */
  @Deprecated
  public abstract void shutdown();

  //@SuppressWarnings("deprecation")
  public void close() throws IOException {
    shutdown();
  }
  
  protected void throwSolrServerOrIOOrRuntimeException(Exception e) throws SolrServerException, IOException {
    if (e instanceof SolrServerException) throw (SolrServerException)e;
    if (e instanceof IOException) throw (IOException)e;
    // including SolrException
    if (e instanceof RuntimeException) throw (RuntimeException)e;
    throw new SolrServerException(e);
  }
  
  protected void throwSolrServerOrRuntimeException(Exception e) throws SolrServerException {
    if (e instanceof SolrServerException) throw (SolrServerException)e;
    // including SolrException
    if (e instanceof RuntimeException) throw (RuntimeException)e;
    throw new SolrServerException(e);
  }

}
