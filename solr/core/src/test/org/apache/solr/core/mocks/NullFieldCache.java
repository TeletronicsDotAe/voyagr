package org.apache.solr.core.mocks;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.uninverting.FieldCache;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

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

public class NullFieldCache implements FieldCache {

  @Override
  public Bits getDocsWithField(LeafReader reader, String field)
      throws IOException {
    return null;
  }

  @Override
  public NumericDocValues getNumerics(LeafReader reader, String field,
      Parser parser, boolean setDocsWithField) throws IOException {
    return null;
  }

  @Override
  public BinaryDocValues getTerms(LeafReader reader, String field,
      boolean setDocsWithField) throws IOException {
    return null;
  }

  @Override
  public BinaryDocValues getTerms(LeafReader reader, String field,
      boolean setDocsWithField, float acceptableOverheadRatio)
      throws IOException {
    return null;
  }

  @Override
  public SortedDocValues getTermsIndex(LeafReader reader, String field)
      throws IOException {
    return null;
  }

  @Override
  public SortedDocValues getTermsIndex(LeafReader reader, String field,
      float acceptableOverheadRatio) throws IOException {
    return null;
  }

  @Override
  public SortedSetDocValues getDocTermOrds(LeafReader reader, String field,
      BytesRef prefix) throws IOException {
    return null;
  }

  @Override
  public CacheEntry[] getCacheEntries() {
    return null;
  }

  @Override
  public void purgeAllCaches() {}

  @Override
  public void purgeByCacheKey(Object coreCacheKey) {}

  @Override
  public void setInfoStream(PrintStream stream) {}

  @Override
  public PrintStream getInfoStream() {
    return null;
  }

}
