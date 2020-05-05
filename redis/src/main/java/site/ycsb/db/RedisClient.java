/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
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

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  // Dispatches to the proper methods of the Lettuce implementation.
  class JustDoTheRightThing {
    private boolean isClusterMode = false;

    private StatefulRedisConnection<String, String> connection;
    private StatefulRedisClusterConnection<String, String> clusterConnection;

    private RedisCommands<String, String> redisCommands;
    private RedisClusterCommands<String, String> redisClusterCommands;

    public JustDoTheRightThing(StatefulRedisConnection<String, String> connection) {
      isClusterMode = false;
      this.connection = connection;
      redisCommands = connection.sync();
    }

    public JustDoTheRightThing(StatefulRedisClusterConnection<String, String> connection) {
      isClusterMode = true;
      clusterConnection= connection;
      redisClusterCommands = connection.sync();
    }

    public void closeConnection() {
      if (isClusterMode) {
        clusterConnection.close();
      } else {
        connection.close();
      }
    }

    public Map<String, String> hgetall(String key) {
      if (isClusterMode) {
        return redisClusterCommands.hgetall(key);
      } else {
        return redisCommands.hgetall(key);
      }
    }

    public List<KeyValue<String, String>> hmget(String key, String... fields) {
      if (isClusterMode) {
        return redisClusterCommands.hmget(key, fields);
      } else {
        return redisCommands.hmget(key, fields);
      }
    }

    public String hmset(String key, Map<String, String> map) {
      if (isClusterMode) {
        return redisClusterCommands.hmset(key, map);
      } else {
        return redisCommands.hmset(key, map);
      }
    }

    public Long zadd(String key, double score, String member) {
      if (isClusterMode) {
        return redisClusterCommands.zadd(key, score, member);
      } else {
        return redisCommands.zadd(key, score, member);
      }
    }

    public Long del(String... keys) {
      if (isClusterMode) {
        return redisClusterCommands.del(keys);
      } else {
        return redisCommands.del(keys);
      }
    }

    public Long zrem(String key, String... members) {
      if (isClusterMode) {
        return redisClusterCommands.zrem(key, members);
      } else {
        return redisCommands.zrem(key, members);
      }
    }

    public List<String> zrangebyscore(String key, double min, double max,
        long offset, long count) {
      if (isClusterMode) {
        return redisClusterCommands.zrangebyscore(key, min, max, offset, count);
      } else {
        return redisCommands.zrangebyscore(key, min, max, offset, count);
      }
    }
  }

  private AbstractRedisClient redisClient;
  private JustDoTheRightThing commands;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String UNIX_SOCKET_PROPERTY = "redis.uds";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";

  public static final String INDEX_KEY = "_indices";

  public void init() throws DBException {
    Properties props = getProperties();
    int port = RedisURI.DEFAULT_REDIS_PORT;

    String udsString = props.getProperty(UNIX_SOCKET_PROPERTY);
    String portString = props.getProperty(PORT_PROPERTY);
    String host = props.getProperty(HOST_PROPERTY);
    String password = props.getProperty(PASSWORD_PROPERTY);
    if (udsString == null && portString != null) {
      port = Integer.parseInt(portString);
    }

    boolean isClusterMode = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    if (isClusterMode) {
      assert(host != null);
      RedisURI.Builder uriBuilder = RedisURI.Builder.redis(host, port);

      if (password != null) {
        uriBuilder = uriBuilder.withPassword(password);
      }

      RedisURI redisUri = uriBuilder.build();
      RedisClusterClient client = RedisClusterClient.create(redisUri);
      redisClient = client;
      commands = new JustDoTheRightThing(client.connect());
    } else {
      RedisURI.Builder uriBuilder;

      if (udsString != null) {
        uriBuilder = RedisURI.Builder.socket(udsString);
      } else {
        uriBuilder = RedisURI.Builder.redis(host, port);
      }

      if (password != null) {
        uriBuilder = uriBuilder.withPassword(password);
      }

      RedisURI redisUri = uriBuilder.build();
      io.lettuce.core.RedisClient client = io.lettuce.core.RedisClient.create(redisUri);
      redisClient = client;
      commands = new JustDoTheRightThing(client.connect());
    }
  }

  public void cleanup() throws DBException {
    commands.closeConnection();
    redisClient.shutdown();
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX commands.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, commands.hgetall(key));
    } else {
      String[] fieldArray =
          (String[]) fields.toArray(new String[fields.size()]);
      List<KeyValue<String, String>> values = commands.hmget(key, fieldArray);

      Iterator<KeyValue<String, String>> it = values.iterator();

      while (it.hasNext()) {
        result.put(it.next().getKey(), new StringByteIterator(it.next().getValue()));
      }
      assert !it.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    if (commands.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK")) {
      commands.zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return commands.del(key) == 0 && commands.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    return commands.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    List<String> keys = commands.zrangebyscore(INDEX_KEY, hash(startkey),
        Double.POSITIVE_INFINITY, 0, recordcount);

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }

}
