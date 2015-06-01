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

import org.apache.commons.io.IOUtils;
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
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;
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
  
  public RestTestHarness(RESTfulServerProvider serverProvider) {
    this.serverProvider = serverProvider;
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
  
  protected static AuthCredentials getAuthCredentials(HttpUriRequest request) {
    String path = request.getURI().getPath();
    if (path.startsWith("/update")) return UPDATE_CREDENTIALS;
    if (path.startsWith("/search") || path.startsWith("/terms") || path.startsWith("/get")) return SEARCH_CREDENTIALS;
    return ALL_CREDENTIALS;
  }
  
  public HttpContext getHttpContextForRequest(HttpUriRequest request) {
    return getHttpContextForRequest(request, getBaseURL());
  }

  public static HttpContext getHttpContextForRequest(HttpUriRequest request, String baseURL) {
    return HttpSolrClient.getHttpContext(getAuthCredentials(request), true, baseURL);
  }

  /**
   * Executes the given request and returns the response.
   */
  public String getResponse(HttpUriRequest request) throws IOException {
    return getResponse(httpClient, request, getHttpContextForRequest(request));
  }
  
  public static String getResponse(CloseableHttpClient httpClient, HttpUriRequest request, HttpContext context) throws IOException {
    RawResponseAndCharset rrac = getRawResponseAndCharset(httpClient, request, context);
    return new String(rrac.rawResponse, rrac.charset);
  }
  
  public byte[] getRawResponse(HttpUriRequest request) throws IOException {
    return getRawResponse(httpClient, request, getHttpContextForRequest(request));
  }
  
  public static byte[] getRawResponse(CloseableHttpClient httpClient, HttpUriRequest request, HttpContext context) throws IOException {
    return getRawResponseAndCharset(httpClient, request, context).rawResponse;
  }
  
  public static final class RawResponseAndCharset {
    public byte[] rawResponse;
    public String charset;
    public RawResponseAndCharset(byte[] rawResponse, String charset) {
      this.rawResponse = rawResponse;
      this.charset = charset;
    }
  }
  
  private static RawResponseAndCharset getRawResponseAndCharset(CloseableHttpClient httpClient, HttpUriRequest request, HttpContext context) throws IOException {
    HttpEntity entity = null;
    try {
      CloseableHttpResponse response = httpClient.execute(request, context);
      try {
      entity = response.getEntity();
      byte[] rawPayload = IOUtils.toByteArray(entity.getContent());
        // Stolen from HttpSolrClient
        int httpStatus = response.getStatusLine().getStatusCode();
        String charset = EntityUtils.getContentCharSet(response.getEntity());
        if (httpStatus != HttpStatus.SC_OK) {
          StringBuilder additionalMsg = new StringBuilder();
          additionalMsg.append( "\n\n" );
          additionalMsg.append( "request: "+request.getURI() );
          NamedList<Object> payload = (response.getFirstHeader(HttpSolrClient.HTTP_EXPLICIT_BODY_INCLUDED_HEADER_KEY) != null)?parseJSON(new String(rawPayload, charset)):null;
          SolrException ex = SolrException.decodeFromHttpMethod(response, "UTF-8", additionalMsg.toString(), payload, rawPayload);
          throw ex;
        }
      return new RawResponseAndCharset(rawPayload, charset);
      } finally {
        response.close();
      }
    } finally {
      EntityUtils.consumeQuietly(entity);
    }
  }
  
  private static NamedList<Object> parseJSON(String JSON) {
    try {
      Map<String, Object> map = (Map)ObjectBuilder.getVal(new JSONParser(new StringReader(JSON)));
      return mapToNamedList(map);
    } catch (Exception e) {
      NamedList<Object> result = new NamedList<Object>();
      result.add("parseException", e);
      return result;
    }
  }
  
  private static NamedList<Object> mapToNamedList(Map<String, Object> map) {
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
