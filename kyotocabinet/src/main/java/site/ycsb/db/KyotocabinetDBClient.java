/*
 * Copyright (c) 2018 - 2019 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import static kyotocabinet.DB.OREADER;
import static kyotocabinet.DB.OWRITER;
import static kyotocabinet.DB.OCREATE;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

// import kyotocabinet.*;

/**
 * Kyoto Cabinet binding for <a href="https://fallabs.com/kyotocabinet/">Kyoto Cabinet</a>.
 */
public class KyotocabinetDBClient extends DB {

  /* https://fallabs.com/kyotocabinet/javadoc/kyotocabinet/DB.html#open(java.lang.String,%20int)
  * for open(..) path parameter
  */
  private static final String PROPERTY_KYOTOCABINETDB_DIR = "kc.dir";
  private static final String PROPERTY_FIELDCOUNT = "fieldcount";
  private static final String PROPERTY_FIELDLENGTH = "fieldlength";
  private static final String PROPERTY_READALLFIELDS = "readallfields";

  private static final String KYOTOCABINETDB_DIR_DEFULAT = "/tmp/kc.kch";
  private static final String FIELDCOUNT_DEFAULT = "10"; // YCSB default
  private static final String FIELDLENGTH_DEFAULT = "100"; // YCSB default
  private static final String READALLFIELDS_DEFAULT = "true"; // YCSB default

  // configuration variables
  private static String kcDbDir = "/tmp/kc.kch";
  private static int ycsbFieldcount = 0, ycsbFieldlength = 0;
  private static boolean ycsbReadallfields = true;

  // internal variables
  private static kyotocabinet.DB kcDb = null;

  @Override
  public void init() throws DBException {

    if(kcDb == null) {
      Properties props = getProperties();

      kcDbDir = props.getProperty(PROPERTY_KYOTOCABINETDB_DIR, KYOTOCABINETDB_DIR_DEFULAT);
      ycsbFieldcount = Integer.parseInt(props.getProperty(PROPERTY_FIELDCOUNT, FIELDCOUNT_DEFAULT));
      ycsbFieldlength = Integer.parseInt(props.getProperty(PROPERTY_FIELDLENGTH, FIELDLENGTH_DEFAULT));
      ycsbReadallfields = Boolean.parseBoolean(props.getProperty(PROPERTY_READALLFIELDS, READALLFIELDS_DEFAULT));

      System.err.printf("%s: %s\n", PROPERTY_KYOTOCABINETDB_DIR, kcDbDir);
      System.err.printf("%s: %d\n", PROPERTY_FIELDCOUNT, ycsbFieldcount);
      System.err.printf("%s: %d\n", PROPERTY_FIELDLENGTH, ycsbFieldlength);
      System.err.printf("%s: %s\n", PROPERTY_READALLFIELDS, ycsbReadallfields);

      if(!(ycsbReadallfields || ycsbFieldcount <= 1)) {
        throw new IllegalArgumentException("Illegal options for kyotocabinet:" +
          "(readallfields || ycsbFieldcount <= 1) should be true.");
      }

      kcDb = new kyotocabinet.DB();

      long startTime = System.nanoTime();
      if (!kcDb.open(kcDbDir.toString(), OREADER | OWRITER | OCREATE)) {
        System.err.printf("open error: %s - %s\n", kcDb.error().name(), kcDb.error());
        throw new IllegalArgumentException();
      }
      long endTime = System.nanoTime();
      long timeElapsed = endTime - startTime;
      System.out.println("[OVERALL], CreatePmemPool(ms), " + timeElapsed / 1000000);
    }
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();

    kcDb.close();
    kcDb = null;
    kcDbDir = null;
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    try {
      assert(fields == null);
    } catch (AssertionError ae) {
      throw new IllegalArgumentException("read(..) null expected for `fields` parameter");
    }

    try {
      // ignore table. There is no table in KCDB
      String value = kcDb.get(key);
      if(value == null) {
        return Status.NOT_FOUND;
      } else {
        result.put(key, new StringByteIterator(value));
      }
      return Status.OK;
    } catch(final kyotocabinet.Error e) {
      System.out.printf("%s: %s\n", e.name(), e.message());
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
        final Vector<HashMap<String, ByteIterator>> result) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      String kcVal = values2value(values);
      if(!kcDb.set(key, kcVal)) {
        System.err.printf("set error: %s - %s\n", kcDb.error().name(), kcDb.error());
        throw new IllegalArgumentException();
      }
      return Status.OK;
    } catch (Exception e){
      System.err.println(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    throw new UnsupportedOperationException();
  }

  public String values2value(Map<String, ByteIterator> values) {
    String value = "";
    for(ByteIterator v : values.values()) {
      value += v.toString();
    }
    assert(new StringByteIterator(value).bytesLeft() == ycsbFieldcount * ycsbFieldlength);
    return value;
  }
}
