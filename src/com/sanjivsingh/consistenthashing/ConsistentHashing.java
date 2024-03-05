package com.sanjivsingh.consistenthashing;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;

/*
 * ConsistentHashing represents implementation of consistent hashing algorithm.
 */

/**
 * The Class ConsistentHashing.
 */
public class ConsistentHashing {
  
  /** The total range. */
  private int totalRange = 50;
  
  /** The node keys. */
  private List<Integer> nodeKeys;
  
  /** The nodes. */
  private List<Server> nodes;
  
  /**
   * Instantiates a new consistent hashing.
   */
  public ConsistentHashing() {
    super();
    this.nodeKeys = new ArrayList<>();     // indices taken up in the ring
    this.nodes = new ArrayList<>();        // nodes present in the ring. nodes.get(i) is present at index
                                           // nodeKeys.get(i)
    this.totalRange = 50;                  // total Range in the ring
  }
  
  /**
   * Adds the node.
   *
   * @param node the node
   * @return the int
   * @throws Exception the exception
   */
  /*
   * addNode function adds a new node in the system and returns the key from the hash space where it was placed
   */
  public int addNode(Server node) throws Exception {
    // handling error when hash space is full.
    if (this.nodeKeys.size() == this.totalRange) {
      throw new Exception("hash space is full");
    }
    
    int key = hash256(node.host, this.totalRange);
    
    /*
     * find the index where the key should be inserted in the keys array this will be the index where the Storage Node
     * will be added in the nodes array.
     */
    int index = bisect(this.nodeKeys, key);
    
    /*
     * if we have already seen the key i.e. node already is present for the same key, we raise Collision Exception
     */
    
    if (index > 0 && this.nodeKeys.get(index - 1) == key) {
      throw new Exception("Collision occurred");
    }
    
    /*
     * Perform data migration i.e. split load from one if existing server.
     */
    if (this.nodeKeys.size() > 0) {
      int indexRight = this.nodeKeys.size() == index ? 0 : index;
      int indexLeft = index > 0 ? index - 1 : this.nodeKeys.size() - 1;
      Server currentNode = this.nodes.get(indexRight);
      split(node, currentNode, this.nodeKeys.get(indexLeft), key);
    }
    
    /*
     * insert the node and the key at the same `index` location. this insertion will keep nodes and keys sorted w.r.t
     * keys.
     */
    this.nodes.add(index, node);
    this.nodeKeys.add(index, key);
    
    return key;
    
  }
  
  /**
   * Removes the node.
   *
   * @param node the node
   * @return the int
   * @throws Exception the exception
   */
  /*
   * remove_node removes the node and returns the key from the hash space on which the node was placed.
   */
  public int removeNode(Server node) throws Exception {
    
    // handling error when space is empty
    if (this.nodeKeys.isEmpty()) {
      throw new Exception("hash space is empty");
    }
    
    int key = hash256(node.host, this.totalRange);
    
    /*
     * we find the index where the key would reside in the keys
     */
    int index = bisectLeft(this.nodeKeys, key);
    int indexRight = (index + 1) % this.nodeKeys.size();
    
    /*
     * if key does not exist in the array we raise Exception
     */
    if (index >= this.nodeKeys.size() || this.nodeKeys.get(index) != key) {
      throw new Exception("node does not exist");
    }
    
    /*
     * Perform data migration: copy data to right server before deleting from cluster.
     */
    copy(node, this.nodes.get(indexRight));
    
    /*
     * now that all sanity checks are done we popping the keys and nodes at the index and thus removing presence of the
     * node.
     */
    this.nodeKeys.remove(index);
    this.nodes.remove(index);
    
    return key;
  }
  
  /**
   * Split.
   *
   * @param newNode the new node
   * @param existingNode the existing node
   * @param left the left
   * @param right the right
   */
  /*
   * copies data from existingNode to newNode between key range [left,right]
   */
  public void split(Server newNode, Server existingNode, int left, int right) {
    
    Set<Integer> moveList = new HashSet<>();
    for (Integer k1 : existingNode.entities.keySet()) {
      if (left < k1 && k1 <= right) {
        moveList.add(k1);
      }
    }
    for (Integer k1 : moveList) {
      Map<String, String> remove = existingNode.entities.remove(k1);
      newNode.entities.put(k1, remove);
    }
  }
  
  /**
   * Copy.
   *
   * @param node1 the node 1
   * @param node2 the node 2
   */
  /*
   * copies data from node1 to node2
   */
  private void copy(Server node1, Server node2) {
    for (Integer key : node1.entities.keySet()) {
      node2.entities.put(key, node1.entities.get(key));
    }
    
  }
  
  /**
   * Adds the entry.
   *
   * @param entityKey the entity key
   * @param entityValue the entity value
   * @throws Exception the exception
   */
  public void addEntry(String entityKey, String entityValue) throws Exception {
    if (entityKey == null) {
      throw new Exception("entityKey is null");
    }
    Server node = this.assign(entityKey);
    int hash256 = hash256(entityKey, this.totalRange);
    node.save(hash256, entityKey, entityValue);
  }
  
  /**
   * Gets the entry.
   *
   * @param entityKey the entity key
   * @return the entry
   * @throws Exception the exception
   */
  public String getEntry(String entityKey) throws Exception {
    if (entityKey == null) {
      throw new Exception("entityKey is null");
    }
    return this.assign(entityKey).search(hash256(entityKey, this.totalRange), entityKey);
  }
  
  /**
   * Removes the entry.
   *
   * @param entityKey the entity key
   * @return the string
   * @throws Exception the exception
   */
  public String removeEntry(String entityKey) throws Exception {
    if (entityKey == null) {
      throw new Exception("entityKey is null");
    }
    return this.assign(entityKey).remove(hash256(entityKey, this.totalRange), entityKey);
  }
  
  /**
   * Assign.
   *
   * @param entityKey the entity key
   * @return the server
   */
  /*
   * Given an item,the function returns the node it is associated with.
   */
  public Server assign(String entityKey) {
    int key = hash256(entityKey, this.totalRange);
    
    /*
     * we find the first node to the right of this key if bisect_right returns index which is out of bounds then we
     * circle back to the first in the array in a circular fashion.
     */
    
    int index = bisectRight(this.nodeKeys, key) % this.nodeKeys.size();
    
    /*
     * return the node present at the index
     */
    return this.nodes.get(index);
  }
  
  /**
   * Hash 256.
   *
   * @param key the key
   * @param totalRange the total range
   * @return the int
   */
  private int hash256(String key, int totalRange) {
    // commons-codec library to get SHA-256 HAX for key
    String sha256hex = DigestUtils.sha256Hex(key);
   
    // converting the HEX digest into equivalent integer value
    BigInteger bigInteger = new BigInteger(sha256hex, 16);
    return Math.abs(bigInteger.intValue() % totalRange);
  }
  
  /**
   * Bisect.
   *
   * @param A the a
   * @param x the x
   * @return the int
   */
  private int bisect(List<Integer> A, int x) {
    return bisectRight(A, x, 0, A.size());
  }
  
  /**
   * Bisect right.
   *
   * @param A the a
   * @param x the x
   * @return the int
   */
  private int bisectRight(List<Integer> A, int x) {
    return bisectRight(A, x, 0, A.size());
  }
  
  /**
   * Bisect right.
   *
   * @param arr the arr
   * @param x the x
   * @param lo the lo
   * @param hi the hi
   * @return the int
   */
  private int bisectRight(List<Integer> arr, int x, int lo, int hi) {
    int N = arr.size();
    if (N == 0) {
      return 0;
    }
    if (x < arr.get(lo)) {
      return lo;
    }
    if (x > arr.get(hi - 1)) {
      return hi;
    }
    for (;;) {
      if (lo + 1 == hi) {
        return lo + 1;
      }
      int mi = (hi + lo) / 2;
      if (x < arr.get(mi)) {
        hi = mi;
      }
      else {
        lo = mi;
      }
    }
  }
  
  /**
   * Bisect left.
   *
   * @param arr the arr
   * @param x the x
   * @return the int
   */
  private int bisectLeft(List<Integer> arr, int x) {
    return bisectLeft(arr, x, 0, arr.size());
  }
  
  /**
   * Bisect left.
   *
   * @param arr the arr
   * @param x the x
   * @param lo the lo
   * @param hi the hi
   * @return the int
   */
  private int bisectLeft(List<Integer> arr, int x, int lo, int hi) {
    int N = arr.size();
    if (N == 0) {
      return 0;
    }
    if (x < arr.get(lo)) {
      return lo;
    }
    if (x > arr.get(hi - 1)) {
      return hi;
    }
    for (;;) {
      if (lo + 1 == hi) {
        return x == arr.get(lo) ? lo : (lo + 1);
      }
      int mi = (hi + lo) / 2;
      if (x <= arr.get(mi)) {
        hi = mi;
      }
      else {
        lo = mi;
      }
    }
  }
  
  /**
   * Prints the.
   */
  public void print() {
    for (int i = 0; i < nodeKeys.size(); i++) {
      System.out.println("Server Hash : " + nodeKeys.get(i));
      System.out.println(nodes.get(i).toString());
    }
  }
  
}
