package org.apache.solr.util;
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

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.security.AuthCredentials;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import java.io.Closeable;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.solr.client.solrj.embedded.JettySolrRunner.*;

/**
 * Facilitates testing Solr's REST API via a provided embedded Jetty
 */
public class RestTestHarness extends BaseTestHarness implements Closeable {
  private RESTfulServerProvider serverProvider;
  private CloseableHttpClient httpClient = HttpClientUtil.createClient(new
      ModifiableSolrParams());
  private AuthCredentials authCredentials;
  
  public RestTestHarness(RESTfulServerProvider serverProvider) {
    this(serverProvider, null, null);
  }
  
  public RestTestHarness(RESTfulServerProvider serverProvider, String username, String password) {
    this.serverProvider = serverProvider;
    if (username != null && password != null) {
      authCredentials = AuthCredentials.createBasicAuthCredentials(username, password);
    }
  }
  
  public String getBaseURL() {
    return serverProvider.getBaseURL();
  }

  public String getAdminURL() {
    return getBaseURL().replace("/collection1", "");
  }
  
  /**
   * Validates an XML "query" response against an array of XPath test strings
   *
   * @param request the Query to process
   * @return null if all good, otherwise the first test that fails.
   * @exception Exception any exception in the response.
   * @exception java.io.IOException if there is a problem writing the XML
   */
  public String validateQuery(String request, String... tests) throws Exception {

    String res = query(request);
    return validateXPath(res, tests);
  }


  /**
   * Validates an XML PUT response against an array of XPath test strings
   *
   * @param request the PUT request to process
   * @param content the content to send with the PUT request
   * @param tests the validating XPath tests
   * @return null if all good, otherwise the first test that fails.
   * @exception Exception any exception in the response.
   * @exception java.io.IOException if there is a problem writing the XML
   */
  public String validatePut(String request, String content, String... tests) throws Exception {

    String res = put(request, content);
    return validateXPath(res, tests);
  }


  /**
   * Processes a "query" using a URL path (with no context path) + optional query params,
   * e.g. "/schema/fields?indent=on"
   *
   * @param request the URL path and optional query params
   * @return The response to the query
   * @exception Exception any exception in the response.
   */
  public String query(String request) throws Exception {
    return getResponse(new HttpGet(getBaseURL() + request));
  }

  public String adminQuery(String request) throws Exception {
    return getResponse(new HttpGet(getAdminURL() + request));
  }

  /**
   * Processes a PUT request using a URL path (with no context path) + optional query params,
   * e.g. "/schema/fields/newfield", PUTs the given content, and returns the response content.
   * 
   * @param request The URL path and optional query params
   * @param content The content to include with the PUT request
   * @return The response to the PUT request
   */
  public String put(String request, String content) throws IOException {
    HttpPut httpPut = new HttpPut(getBaseURL() + request);
    httpPut.setEntity(new StringEntity(content, ContentType.create(
        "application/json", StandardCharsets.UTF_8)));
    
    return getResponse(httpPut);
  }

  /**
   * Processes a DELETE request using a URL path (with no context path) + optional query params,
   * e.g. "/schema/analysis/protwords/english", and returns the response content.
   *
   * @param request the URL path and optional query params
   * @return The response to the DELETE request
   */
  public String delete(String request) throws IOException {
    HttpDelete httpDelete = new HttpDelete(getBaseURL() + request);
    return getResponse(httpDelete);
  }

  /**
   * Processes a POST request using a URL path (with no context path) + optional query params,
   * e.g. "/schema/fields/newfield", PUTs the given content, and returns the response content.
   *
   * @param request The URL path and optional query params
   * @param content The content to include with the POST request
   * @return The response to the POST request
   */
  public String post(String request, String content) throws IOException {
    HttpPost httpPost = new HttpPost(getBaseURL() + request);
    httpPost.setEntity(new StringEntity(content, ContentType.create(
        "application/json", StandardCharsets.UTF_8)));
    
    return getResponse(httpPost);
  }


  public String checkResponseStatus(String xml, String code) throws Exception {
    try {
      String response = query(xml);
      String valid = validateXPath(response, "//int[@name='status']="+code );
      return (null == valid) ? null : response;
    } catch (XPathExpressionException e) {
      throw new RuntimeException("?!? static xpath has bug?", e);
    }
  }

  public String checkAdminResponseStatus(String xml, String code) throws Exception {
    try {
      String response = adminQuery(xml);
      String valid = validateXPath(response, "//int[@name='status']="+code );
      return (null == valid) ? null : response;
    } catch (XPathExpressionException e) {
      throw new RuntimeException("?!? static xpath has bug?", e);
    }
  }
  /**
   * Reloads the first core listed in the response to the core admin handler STATUS command
   */
  @Override
  public void reload() throws Exception {
    String coreName = (String)evaluateXPath
        (adminQuery("/admin/cores?action=STATUS"),
         "//lst[@name='status']/lst[1]/str[@name='name']",
         XPathConstants.STRING);
    String xml = checkAdminResponseStatus("/admin/cores?action=RELOAD&core=" + coreName, "0");
    if (null != xml) {
      throw new RuntimeException("RELOAD failed:\n" + xml);
    }
  }

  /**
   * Processes an "update" (add, commit or optimize) and
   * returns the response as a String.
   *
   * @param xml The XML of the update
   * @return The XML response to the update
   */
  @Override
  public String update(String xml) {
    try {
      return query("/update?stream.body=" + URLEncoder.encode(xml, "UTF-8"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  protected AuthCredentials getAuthCredentials(HttpUriRequest request) {
    String path = request.getURI().getPath();
    if (path.startsWith("/update")) return UPDATE_CREDENTIALS;
    if (path.startsWith("/search") || path.startsWith("/terms") || path.startsWith("/get")) return SEARCH_CREDENTIALS;
    return ALL_CREDENTIALS;
  }
  
  public HttpContext getHttpContextForRequest(HttpUriRequest request) {
    return HttpSolrClient.getHttpContext(getAuthCredentials(request), true, getBaseURL());
  }

  /**
   * Executes the given request and returns the response.
   */
  private String getResponse(HttpUriRequest request) throws IOException {
    HttpEntity entity = null;
    try {
      final HttpContext context = getHttpContextForRequest(request);
      CloseableHttpResponse response = httpClient.execute(request, context);
      try {
      entity = response.getEntity();
      String JSONStrPayload = EntityUtils.toString(entity, StandardCharsets.UTF_8);
        // Stolen from HttpSolrClient
        int httpStatus = response.getStatusLine().getStatusCode();
        String charset = EntityUtils.getContentCharSet(response.getEntity());
        if (httpStatus != HttpStatus.SC_OK) {
          StringBuilder additionalMsg = new StringBuilder();
          additionalMsg.append( "\n\n" );
          additionalMsg.append( "request: "+request.getURI() );
          NamedList<Object> payload = (response.getFirstHeader(HttpSolrClient.HTTP_EXPLICIT_BODY_INCLUDED_HEADER_KEY) != null)?parseJSON(JSONStrPayload):null;
          SolrException ex = SolrException.decodeFromHttpMethod(response, "UTF-8", additionalMsg.toString(), payload);
          throw ex;
        }
      return JSONStrPayload;
      } finally {
        response.close();
      }
    } finally {
      EntityUtils.consumeQuietly(entity);
    }
  }
  
  private NamedList<Object> parseJSON(String JSON) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> map = mapper.readValue(JSON, 
        new TypeReference<HashMap<String,Object>>(){});
    return mapToNamedList(map);
  }
  
  private NamedList<Object> mapToNamedList(Map<String, Object> map) {
    NamedList<Object> result = new NamedList<Object>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Map) value = mapToNamedList((Map<String, Object>)value);
      result.add(entry.getKey(), value);
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }
}
