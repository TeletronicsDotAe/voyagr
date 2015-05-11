package org.apache.solr.handler.component;

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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.Assert;
import org.apache.solr.common.util.StrUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test for QueryComponent's distributed querying optimization.
 * If the "fl" param is just "id" or just "id,score", all document data to return is already fetched by STAGE_EXECUTE_QUERY.
 * The second STAGE_GET_FIELDS query is completely unnecessary.
 * Eliminating that 2nd HTTP request can make a big difference in overall performance.
 *
 * @see QueryComponent
 */
public class DistributedQueryComponentOptimizationTest extends AbstractFullDistribZkTestBase {
  static Logger log = LoggerFactory.getLogger(DistributedQueryComponentOptimizationTest.class);

  public DistributedQueryComponentOptimizationTest() {
    stress = 0;
    schemaString = "schema-custom-field.xml";
  }

  @Override
  protected String getSolrXml() {
    return "solr-trackingshardhandler.xml";
  }

  @Test
  @ShardsFixed(num = 3)
  public void test() throws Exception {
    // This test is explicitly testing DQA. Make sure we run with the real DefaultProvider
    switchToOriginalDQADefaultProvider();
    waitForThingsToLevelOut(30);
    del("*:*");

    index(id, "1", "text", "a", "test_sS", "21", "payload", ByteBuffer.wrap(new byte[]{0x12, 0x62, 0x15}),                     //  2
        // quick check to prove "*" dynamicField hasn't been broken by somebody mucking with schema
        "asdfasdf_field_should_match_catchall_dynamic_field_adsfasdf", "value");
    index(id, "2", "text", "b", "test_sS", "22", "payload", ByteBuffer.wrap(new byte[]{0x25, 0x21, 0x16}));                    //  5
    index(id, "3", "text", "a", "test_sS", "23", "payload", ByteBuffer.wrap(new byte[]{0x35, 0x32, 0x58}));                    //  8
    index(id, "4", "text", "b", "test_sS", "24", "payload", ByteBuffer.wrap(new byte[]{0x25, 0x21, 0x15}));                    //  4
    index(id, "5", "text", "a", "test_sS", "25", "payload", ByteBuffer.wrap(new byte[]{0x35, 0x35, 0x10, 0x00}));              //  9
    index(id, "6", "text", "c", "test_sS", "26", "payload", ByteBuffer.wrap(new byte[]{0x1a, 0x2b, 0x3c, 0x00, 0x00, 0x03}));  //  3
    index(id, "7", "text", "c", "test_sS", "27", "payload", ByteBuffer.wrap(new byte[]{0x00, 0x3c, 0x73}));                    //  1
    index(id, "8", "text", "c", "test_sS", "28", "payload", ByteBuffer.wrap(new byte[]{0x59, 0x2d, 0x4d}));                    // 11
    index(id, "9", "text", "a", "test_sS", "29", "payload", ByteBuffer.wrap(new byte[]{0x39, 0x79, 0x7a}));                    // 10
    index(id, "10", "text", "b", "test_sS", "30", "payload", ByteBuffer.wrap(new byte[]{0x31, 0x39, 0x7c}));                   //  6
    index(id, "11", "text", "d", "test_sS", "31", "payload", ByteBuffer.wrap(new byte[]{(byte) 0xff, (byte) 0xaf, (byte) 0x9c})); // 13
    index(id, "12", "text", "d", "test_sS", "32", "payload", ByteBuffer.wrap(new byte[]{0x34, (byte) 0xdd, 0x4d}));             //  7
    index(id, "13", "text", "d", "test_sS", "33", "payload", ByteBuffer.wrap(new byte[]{(byte) 0x80, 0x11, 0x33}));             // 12
    commit();

    List<String[]> queryParamsCombinationsToTest = new ArrayList<String[]>();
    queryParamsCombinationsToTest.add(new String[]{"q", "*:*", "fl", "id,test_sS,score", "sort", "payload desc", "rows", "20"});
    queryParamsCombinationsToTest.add(new String[]{"q", "*:*", "fl", "id,test_sS,score", "sort", "payload asc", "rows", "20"});
    queryParamsCombinationsToTest.add(new String[]{"q", "*:*", "fl", "id,score", "sort", "payload desc", "rows", "20"});
    queryParamsCombinationsToTest.add(new String[]{"q", "*:*", "fl", "id,score", "sort", "payload asc", "rows", "20"});
    queryParamsCombinationsToTest.add(new String[]{"q", "*:*", "fl", "id", "sort", "payload desc", "rows", "20"});
    queryParamsCombinationsToTest.add(new String[]{"q", "*:*", "fl", "id", "sort", "payload asc", "rows", "20"});
    queryParamsCombinationsToTest.add(new String[]{"q", "*:*", "fl", "score", "sort", "payload asc", "rows", "20"});

    for (String[] paramObjs : queryParamsCombinationsToTest) {
      QueryResponse rsp = query(paramObjs);
      SolrParams params = params(paramObjs);
      if (params.get("sort").equals("payload asc") && !params.get(CommonParams.FL).equals("score")) {
        assertFieldValues(rsp.getResults(), id, 7, 1, 6, 4, 2, 10, 12, 3, 5, 9, 8, 13, 11);
      } else if (params.get("sort").equals("payload desc")) {
        assertFieldValues(rsp.getResults(), id, 11, 13, 8, 9, 5, 3, 12, 10, 2, 4, 6, 1, 7);
      }
      if (params.get("fl").contains("test_sS")) {
        if (params.get("sort").equals("payload asc")) {
          assertFieldValues(rsp.getResults(), "test_sS", "27", "21", "26", "24", "22", "30", "32", "23", "25", "29", "28", "33", "31");
        } else if (params.get("sort").equals("payload desc")) {
          assertFieldValues(rsp.getResults(), "test_sS", "31", "33", "28", "29", "25", "23", "32", "30", "22", "24", "26", "21", "27");
        }
      }
      for (ShardParams.DQA dqa : ShardParams.DQA.values()) {
        String[] moddedParamObjs = new String[paramObjs.length + 4];
        System.arraycopy(paramObjs, 0, moddedParamObjs, 0, paramObjs.length);
        moddedParamObjs[moddedParamObjs.length - 4] = ShardParams.DQA.QUERY_PARAM;
        moddedParamObjs[moddedParamObjs.length - 3] = (random().nextBoolean()) ? dqa.getId() : dqa.getAliasIds()[0];
        moddedParamObjs[moddedParamObjs.length - 2] = "not-used";
        moddedParamObjs[moddedParamObjs.length - 1] = "not-used";

        queryWithAsserts(dqa, params(moddedParamObjs));
        // QueryResponse rsp2 = query(moddedParamObjs);
        // compareResponses(rsp, rsp2, dqa);

        moddedParamObjs[moddedParamObjs.length - 2] = ShardParams.DQA.FORCE_SKIP_GET_IDS_PARAM;
        moddedParamObjs[moddedParamObjs.length - 1] = "true";
        queryWithAsserts(dqa, params(moddedParamObjs));
//        rsp2 = query(moddedParamObjs);
//        compareResponses(rsp, rsp2, dqa);

        moddedParamObjs[moddedParamObjs.length - 2] = ShardParams.DQA.FORCE_SKIP_GET_IDS_PARAM;
        moddedParamObjs[moddedParamObjs.length - 1] = "false";
        queryWithAsserts(dqa, params(moddedParamObjs));
//        rsp2 = query(moddedParamObjs);
//        compareResponses(rsp, rsp2, dqa);

      }
    }

    // SOLR-6545, wild card field list
    index(id, "19", "text", "d", "cat_a_sS", "1", "dynamic", "2", "payload", ByteBuffer.wrap(new byte[]{(byte) 0x80, 0x11, 0x34}));
    commit();

    queryParamsCombinationsToTest = new ArrayList<>();
    queryParamsCombinationsToTest.add(new String[]{"q", "id:19", "fl", "id,*a_sS", "sort", "payload asc"});
    queryParamsCombinationsToTest.add(new String[]{"q", "id:19", "fl", "id,dynamic,cat*", "sort", "payload asc"});
    queryParamsCombinationsToTest.add(new String[]{"q", "id:19", "fl", "id,*a_sS", "sort", "payload asc"});
    queryParamsCombinationsToTest.add(new String[]{"q", "id:19", "fl", "id,dynamic,cat*", "sort", "payload asc"});
    // fix for a bug where not all fields are returned if using multiple fl parameters, see SOLR-6796
    queryParamsCombinationsToTest.add(new String[]{"q", "*:*", "fl", "id", "fl", "dynamic", "sort", "payload desc"});
    // missing fl with sort
    queryParamsCombinationsToTest.add(new String[]{"q", "*:*", "sort", "payload desc"});
    // fl=*
    queryParamsCombinationsToTest.add(new String[]{"q", "*:*", "fl", "*", "sort", "payload desc"});
    queryParamsCombinationsToTest.add(new String[]{"q", "*:*", "fl", "*,score", "sort", "payload desc"});

    for (String[] paramObjs : queryParamsCombinationsToTest) {
      QueryResponse rsp = query(paramObjs);
//      assertFieldValues(rsp.getResults(), "id", 19);
      for (ShardParams.DQA dqa : ShardParams.DQA.values()) {
        String[] moddedParamObjs = new String[paramObjs.length + 4];
        for (int i = 0; i < paramObjs.length; i++) {
          moddedParamObjs[i] = paramObjs[i];
        }
        moddedParamObjs[moddedParamObjs.length - 4] = ShardParams.DQA.QUERY_PARAM;
        moddedParamObjs[moddedParamObjs.length - 3] = (random().nextBoolean()) ? dqa.getId() : dqa.getAliasIds()[0];
        moddedParamObjs[moddedParamObjs.length - 2] = "not-used";
        moddedParamObjs[moddedParamObjs.length - 1] = "not-used";

//        QueryResponse rsp2 = query(moddedParamObjs);
//        compareResponses(rsp, rsp2, dqa);
        queryWithAsserts(dqa, params(moddedParamObjs));

        moddedParamObjs[moddedParamObjs.length - 2] = ShardParams.DQA.FORCE_SKIP_GET_IDS_PARAM;
        moddedParamObjs[moddedParamObjs.length - 1] = "true";
        queryWithAsserts(dqa, params(moddedParamObjs));
//        rsp2 = query(moddedParamObjs);
//        compareResponses(rsp, rsp2, dqa);

        moddedParamObjs[moddedParamObjs.length - 2] = ShardParams.DQA.FORCE_SKIP_GET_IDS_PARAM;
        moddedParamObjs[moddedParamObjs.length - 1] = "false";
        queryWithAsserts(dqa, params(moddedParamObjs));
//        rsp2 = query(moddedParamObjs);
//        compareResponses(rsp, rsp2, dqa);
      }
    }


    // see SOLR-6795, distrib.singlePass=true would return score even when not asked for
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    handle.put("_version_", SKIPVAL);
    // we don't to compare maxScore because most distributed requests return it anyway (just because they have score already)
    handle.put("maxScore", SKIPVAL);
    // this trips the queryWithAsserts function because it uses a custom parser, so just query directly
    query("q", "{!func}id", ShardParams.DQA.FORCE_SKIP_GET_IDS_PARAM, "true");
  }

  /**
   * This test now asserts that every distrib.singlePass query:
   * <ol>
   * <li>Makes exactly 'numSlices' number of shard requests</li>
   * <li>Makes no GET_FIELDS requests</li>
   * <li>Must request the unique key field from shards</li>
   * <li>Must request the score if 'fl' has score or sort by score is requested</li>
   * <li>Requests all fields that are present in 'fl' param</li>
   * </ol>
   * <p>
   * It also asserts that every regular two phase distribtued search:
   * <ol>
   * <li>Makes at most 2 * 'numSlices' number of shard requests</li>
   * <li>Must request the unique key field from shards</li>
   * <li>Must request the score if 'fl' has score or sort by score is requested</li>
   * <li>Requests no fields other than id and score in GET_TOP_IDS request</li>
   * <li>Requests exactly the fields that are present in 'fl' param in GET_FIELDS request and no others</li>
   * </ol>
   * <p>
   * and also asserts that each query which requests id or score or both behaves exactly like a single pass query
   */
  private QueryResponse queryWithAsserts(ShardParams.DQA dqa, ModifiableSolrParams params) throws Exception {
    log.info("Executing query with dqa=" + dqa + " and params: " + params);
    TrackingShardHandlerFactory.RequestTrackingQueue trackingQueue = new TrackingShardHandlerFactory.RequestTrackingQueue();
    // the jettys doesn't include the control jetty which is exactly what we need here
    TrackingShardHandlerFactory.setTrackingQueue(jettys, trackingQueue);

    // let's add debug=track to such requests so we can use DebugComponent responses for assertions
    params.add("debug", "track");

    handle.put("debug", SKIPVAL);
    QueryResponse response = query(params);

    Map<String, List<TrackingShardHandlerFactory.ShardRequestAndParams>> requests = trackingQueue.getAllRequests();
    int numRequests = getNumRequests(requests);

    boolean distribSinglePass = false;

    Set<String> fls = new HashSet<>();
    Set<String> sortFields = new HashSet<>();

    if (dqa == ShardParams.DQA.FIND_ID_RELEVANCE_FETCH_BY_IDS && dqa.forceSkipGetIds(params)) {
      assertTrue("distrib.forceSkipGetIds=true made more requests than number of shards",
          numRequests == sliceCount);
      distribSinglePass = true;
    }
    String[] flsArr = params.getParams(CommonParams.FL);
    if (flsArr != null) {
      for (String s : flsArr) {
        fls.addAll(StrUtils.splitSmart(s, ','));
      }
    }
    sortFields.addAll(StrUtils.splitSmart(StrUtils.splitSmart(params.get(CommonParams.SORT), ' ').get(0), ','));

    Set<String> idScoreFields = new HashSet<>(2);
    idScoreFields.add("id"); // id is always requested in GET_TOP_IDS phase
    // score is optional, requested only if sorted by score
    if (fls.contains("score") || sortFields.contains("score")) idScoreFields.add("score");

    if (dqa == ShardParams.DQA.FIND_ID_RELEVANCE_FETCH_BY_IDS && idScoreFields.containsAll(fls) && !fls.isEmpty()) {
      // if id and/or score are the only fields being requested then we implicitly turn on distribSinglePass=true
      distribSinglePass = true;
    }

    if (dqa == ShardParams.DQA.FIND_ID_RELEVANCE_FETCH_BY_IDS && distribSinglePass) {
      Map<String, Object> debugMap = response.getDebugMap();
      SimpleOrderedMap<Object> track = (SimpleOrderedMap<Object>) debugMap.get("track");
      assertNotNull(track);
      assertNotNull(track.get("EXECUTE_QUERY"));
      assertNull("A single pass request should not have a GET_FIELDS phase", track.get("GET_FIELDS"));

      // all fields should be requested in one go but even if 'id' is not requested by user
      // it must still be fetched in this phase to merge correctly
      Set<String> reqAndIdScoreFields = new HashSet<>(fls);
      reqAndIdScoreFields.addAll(idScoreFields);
      assertParamsEquals(trackingQueue, DEFAULT_COLLECTION, SHARD1,
          CommonParams.FL, ShardRequest.PURPOSE_GET_TOP_IDS, reqAndIdScoreFields.toArray(new String[reqAndIdScoreFields.size()]));
      assertParamsEquals(trackingQueue, DEFAULT_COLLECTION, SHARD2,
          CommonParams.FL, ShardRequest.PURPOSE_GET_TOP_IDS, reqAndIdScoreFields.toArray(new String[reqAndIdScoreFields.size()]));
    } else if (dqa == ShardParams.DQA.FIND_ID_RELEVANCE_FETCH_BY_IDS) {
      if (numRequests > sliceCount * 2) {
        log.error("Request map = {}", trackingQueue.getAllRequests()); // todo nocommit
      }

      // we are assuming there are no facet refinement or distributed idf requests here
      assertTrue("distrib.singlePass=false made more requests than 2 * number of shards." +
              " Actual: " + numRequests + " but expected <= " + sliceCount * 2,
          numRequests <= sliceCount * 2);

      // only id and/or score should be requested
      assertParamsEquals(trackingQueue, DEFAULT_COLLECTION, SHARD1,
          CommonParams.FL, ShardRequest.PURPOSE_GET_TOP_IDS, idScoreFields.toArray(new String[idScoreFields.size()]));
      assertParamsEquals(trackingQueue, DEFAULT_COLLECTION, SHARD2,
          CommonParams.FL, ShardRequest.PURPOSE_GET_TOP_IDS, idScoreFields.toArray(new String[idScoreFields.size()]));

      // only originally requested fields must be requested in GET_FIELDS request
      assertParamsEquals(trackingQueue, DEFAULT_COLLECTION, SHARD1,
          CommonParams.FL, ShardRequest.PURPOSE_GET_FIELDS, fls.toArray(new String[fls.size()]));
      assertParamsEquals(trackingQueue, DEFAULT_COLLECTION, SHARD2,
          CommonParams.FL, ShardRequest.PURPOSE_GET_FIELDS, fls.toArray(new String[fls.size()]));
    } else if (dqa == ShardParams.DQA.FIND_RELEVANCE_FIND_IDS_LIMITED_ROWS_FETCH_BY_IDS)  {

    }

    Map<String, Object> debugMap = response.getDebugMap();
    SimpleOrderedMap<Object> track = (SimpleOrderedMap<Object>) debugMap.get("track");
    if (dqa.equals(ShardParams.DQA.FIND_RELEVANCE_FIND_IDS_LIMITED_ROWS_FETCH_BY_IDS)) {
      assertNotNull(track.get("LIMIT_ROWS"));
    } else {
      assertNull(track.get("LIMIT_ROWS"));
    }

    return response;
  }

  private int getNumRequests(Map<String, List<TrackingShardHandlerFactory.ShardRequestAndParams>> requests) {
    int beforeNumRequests = 0;
    for (Map.Entry<String, List<TrackingShardHandlerFactory.ShardRequestAndParams>> entry : requests.entrySet()) {
      beforeNumRequests += entry.getValue().size();
    }
    return beforeNumRequests;
  }

  private void assertParamsEquals(TrackingShardHandlerFactory.RequestTrackingQueue trackingQueue, String collection, String shard, String paramName, int purpose, String... values) {
    TrackingShardHandlerFactory.ShardRequestAndParams getByIdRequest = trackingQueue.getShardRequestByPurpose(cloudClient.getZkStateReader(), collection, shard, purpose);
    assertParamsEquals(getByIdRequest, paramName, values);
  }

  private void assertParamsEquals(TrackingShardHandlerFactory.ShardRequestAndParams requestAndParams, String paramName, String... values) {
    if (requestAndParams == null) return;
    int expectedCount = values.length;
    String[] params = requestAndParams.params.getParams(paramName);
    if (expectedCount > 0 && (params == null || params.length == 0)) {
      fail("Expected non-zero number of '" + paramName + "' parameters in request");
    }
    Set<String> requestedFields = new HashSet<>();
    if (params != null) {
      for (String p : params) {
        List<String> list = StrUtils.splitSmart(p, ',');
        for (String s : list) {
          // make sure field names aren't duplicated in the parameters
          assertTrue("Field name " + s + " was requested multiple times: params = " + requestAndParams.params,
              requestedFields.add(s));
        }
      }
    }
    // if a wildcard ALL field is requested then we don't need to match exact number of params
    if (!requestedFields.contains("*"))  {
      assertEquals("Number of requested fields do not match with expectations", expectedCount, requestedFields.size());
      for (String field : values) {
        if (!requestedFields.contains(field)) {
          fail("Field " + field + " not found in param: " + paramName + " request had " + paramName + "=" + requestedFields);
        }
      }
    }
  }
}
