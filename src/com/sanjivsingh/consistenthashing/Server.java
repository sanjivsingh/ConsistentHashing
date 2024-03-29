/*
 * Copyright 2024 Sanjiv Singh
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.sanjivsingh.consistenthashing;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.collections4.map.HashedMap;

/**
 * The Class Server.
 */
public class Server {
  
  /** The host. */
  String host;
  
  /** The name. */
  String name;
  
  /** The entities. */
  TreeMap<Integer, Map<String, String>> entities = null;
  
  /**
   * Instantiates a new server.
   *
   * @param name the name
   * @param host the host
   */
  public Server(String name, String host) {
    super();
    this.host = host;
    this.name = name;
    entities = new TreeMap<>();
  }
  
  /**
   * Save.
   *
   * @param key the key
   * @param entityKey the entity key
   * @param entityValue the entity value
   */
  public void save(Integer key, String entityKey, String entityValue) {
    entities.putIfAbsent(key, new HashedMap<>());
    entities.get(key).put(entityKey, entityValue);
  }
  
  /**
   * Search.
   *
   * @param key the key
   * @param entityKey the entity key
   * @return the string
   */
  public String search(Integer key, String entityKey) {
    if (!entities.containsKey(key)) {
      return null;
    }
    return entities.get(key).get(entityKey);
  }
  
  /**
   * Removes the.
   *
   * @param key the key
   * @param entityKey the entity key
   * @return the string
   */
  public String remove(Integer key, String entityKey) {
    if (entities.containsKey(key)) {
      return entities.get(key).remove(entityKey);
    }
    return null;
  }
    
  /**
   * To string.
   *
   * @return the string
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Host : " + host);
    sb.append("\nname : " + name);
    for (Integer hash : entities.keySet()) {
      sb.append("   \n      hash : " + hash + " Value : " + entities.get(hash));
    }
    return sb.toString();
  }
  
}
