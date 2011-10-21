/**
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
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * Keep prioritized queues of under replicated blocks.
 * Blocks have replication priority, with priority 0 indicating the highest
 * priority.
 *
 * <p/>
 * The policy for choosing which priority to give added blocks
 * is implemented in {@link #getPriority(Block, int, int, int)}.
 */
class UnderReplicatedBlocks implements Iterable<Block> {
  /** The total number of queues : {@value} */
  static final int LEVEL = 5;
  /** The queue with the highest priority: {@value} */
  static final int QUEUE_HIGHEST_PRIORITY = 0;
  /** The queue for blocks that are way below their expected value : {@value} */
  static final int QUEUE_VERY_UNDER_REPLICATED = 1;
  /** The queue for "normally" under-replicated blocks: {@value} */
  static final int QUEUE_UNDER_REPLICATED = 2;
  /** The queue for blocks that have the right number of replicas,
   * but which the block manager felt were badly distributed: {@value}
   */
  static final int QUEUE_REPLICAS_BADLY_DISTRIBUTED = 3;
  /** The queue for corrupt blocks: {@value} */
  static final int QUEUE_WITH_CORRUPT_BLOCKS = 4;
  /** the queues themselves */
  private final List<NavigableSet<Block>> priorityQueues
      = new ArrayList<NavigableSet<Block>>(LEVEL);
      
  /** Create an object. */
  UnderReplicatedBlocks() {
    for (int i = 0; i < LEVEL; i++) {
      priorityQueues.add(new TreeSet<Block>());
    }
  }

  /**
   * Empty the queues.
   */
  void clear() {
    for (int i = 0; i < LEVEL; i++) {
      priorityQueues.get(i).clear();
    }
  }

  /** Return the total number of under replication blocks */
  synchronized int size() {
    int size = 0;
    for (int i = 0; i < LEVEL; i++) {
      size += priorityQueues.get(i).size();
    }
    return size;
  }

  /** Return the number of under replication blocks excluding corrupt blocks */
  synchronized int getUnderReplicatedBlockCount() {
    int size = 0;
    for (int i = 0; i < LEVEL; i++) {
      if (i != QUEUE_WITH_CORRUPT_BLOCKS) {
        size += priorityQueues.get(i).size();
      }
    }
    return size;
  }
  
  /** Return the number of corrupt blocks */
  synchronized int getCorruptBlockSize() {
    return priorityQueues.get(QUEUE_WITH_CORRUPT_BLOCKS).size();
  }
  
  /** Check if a block is in the neededReplication queue */
  synchronized boolean contains(Block block) {
    for (NavigableSet<Block> set : priorityQueues) {
      if (set.contains(block)) {
        return true;
      }
    }
    return false;
  }
      
  /** Return the priority of a block
   * @param block a under replicated block
   * @param curReplicas current number of replicas of the block
   * @param expectedReplicas expected number of replicas of the block
   * @return the priority for the blocks, between 0 and ({@link #LEVEL}-1)
   */
  private int getPriority(Block block, 
                          int curReplicas, 
                          int decommissionedReplicas,
                          int expectedReplicas) {
    assert curReplicas >= 0 : "Negative replicas!";
    if (curReplicas >= expectedReplicas) {
      // Block has enough copies, but not enough racks
      return QUEUE_REPLICAS_BADLY_DISTRIBUTED;
    } else if (curReplicas == 0) {
      // If there are zero non-decommissioned replica but there are
      // some decommissioned replicas, then assign them highest priority
      if (decommissionedReplicas > 0) {
        return QUEUE_HIGHEST_PRIORITY;
      }
      return QUEUE_WITH_CORRUPT_BLOCKS; // keep these blocks in needed replication.
    } else if (curReplicas == 1) {
      return QUEUE_HIGHEST_PRIORITY; // highest priority
    } else if ((curReplicas * 3) < expectedReplicas) {
      //there is less than a third as many blocks as requested;
      //this is considered very under-replicated
      return QUEUE_VERY_UNDER_REPLICATED;
    } else {
      //add to the normal queue for under replicated blocks
      return QUEUE_UNDER_REPLICATED;
    }
  }
      
  /** add a block to a under replication queue according to its priority
   * @param block a under replication block
   * @param curReplicas current number of replicas of the block
   * @param decomissionedReplicas the number of decommissioned replicas
   * @param expectedReplicas expected number of replicas of the block
   * @return true if the block was added to a queue.
   */
  synchronized boolean add(Block block,
                           int curReplicas, 
                           int decomissionedReplicas,
                           int expectedReplicas) {
    assert curReplicas >= 0 : "Negative replicas!";
    int priLevel = getPriority(block, curReplicas, decomissionedReplicas,
                               expectedReplicas);
    if(priLevel != LEVEL && priorityQueues.get(priLevel).add(block)) {
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.UnderReplicationBlock.add:"
          + block
          + " has only " + curReplicas
          + " replicas and need " + expectedReplicas
          + " replicas so is added to neededReplications"
          + " at priority level " + priLevel);
      }
      return true;
    }
    return false;
  }

  /** remove a block from a under replication queue */
  synchronized boolean remove(Block block, 
                              int oldReplicas, 
                              int decommissionedReplicas,
                              int oldExpectedReplicas) {
    int priLevel = getPriority(block, oldReplicas, 
                               decommissionedReplicas,
                               oldExpectedReplicas);
    return remove(block, priLevel);
  }
      
  /**
   * Remove a block from the under replication queues.
   *
   * The priLevel parameter is a hint of which queue to query
   * first: if negative or &gt;= {@link #LEVEL} this shortcutting
   * is not attmpted.
   *
   * If the block is not found in the nominated queue, an attempt is made to
   * remove it from all queues.
   *
   * <i>Warning:</i> This is not a synchronized method.
   * @param block block to remove
   * @param priLevel expected privilege level
   * @return true if the block was found and removed from one of the priority queues
   */
  boolean remove(Block block, int priLevel) {
    if(priLevel >= 0 && priLevel < LEVEL 
        && priorityQueues.get(priLevel).remove(block)) {
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.UnderReplicationBlock.remove: "
          + "Removing block " + block
          + " from priority queue "+ priLevel);
      }
      return true;
    } else {
      // Try to remove the block from all queues if the block was
      // not found in the queue for the given priority level.
      for (int i = 0; i < LEVEL; i++) {
        if (priorityQueues.get(i).remove(block)) {
          if(NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug(
              "BLOCK* NameSystem.UnderReplicationBlock.remove: "
              + "Removing block " + block
              + " from priority queue "+ i);
          }
          return true;
        }
      }
    }
    return false;
  }
      
  /**
   * Recalculate and potentially update the priority level of a block.
   *
   * If the block priority has changed from before an attempt is made to
   * remove it from the block queue. Regardless of whether or not the block
   * is in the block queue of (recalculate) priority, an attempt is made
   * to add it to that queue. This ensures that the block will be
   * in its expected priority queue (and only that queue) by the end of the
   * method call.
   * @param block a under replicated block
   * @param curReplicas current number of replicas of the block
   * @param decommissionedReplicas  the number of decommissioned replicas
   * @param curExpectedReplicas expected number of replicas of the block
   * @param curReplicasDelta the change in the replicate count from before
   * @param expectedReplicasDelta the change in the expected replica count from before
   */
  synchronized void update(Block block, int curReplicas,
                           int decommissionedReplicas,
                           int curExpectedReplicas,
                           int curReplicasDelta, int expectedReplicasDelta) {
    int oldReplicas = curReplicas-curReplicasDelta;
    int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
    int curPri = getPriority(block, curReplicas, decommissionedReplicas, curExpectedReplicas);
    int oldPri = getPriority(block, oldReplicas, decommissionedReplicas, oldExpectedReplicas);
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("UnderReplicationBlocks.update " + 
        block +
        " curReplicas " + curReplicas +
        " curExpectedReplicas " + curExpectedReplicas +
        " oldReplicas " + oldReplicas +
        " oldExpectedReplicas  " + oldExpectedReplicas +
        " curPri  " + curPri +
        " oldPri  " + oldPri);
    }
    if(oldPri != LEVEL && oldPri != curPri) {
      remove(block, oldPri);
    }
    if(curPri != LEVEL && priorityQueues.get(curPri).add(block)) {
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.UnderReplicationBlock.update:"
          + block
          + " has only "+ curReplicas
          + " replicas and needs " + curExpectedReplicas
          + " replicas so is added to neededReplications"
          + " at priority level " + curPri);
      }
    }
  }

  /** returns an iterator of all blocks in a given priority queue */
  synchronized BlockIterator iterator(int level) {
    return new BlockIterator(level);
  }
    
  /** return an iterator of all the under replication blocks */
  @Override
  public synchronized BlockIterator iterator() {
    return new BlockIterator();
  }

  /**
   * An iterator over blocks.
   */
  class BlockIterator implements Iterator<Block> {
    private int level;
    private boolean isIteratorForLevel = false;
    private List<Iterator<Block>> iterators = new ArrayList<Iterator<Block>>();

    /**
     * Construct an iterator over all queues.
     */
    private BlockIterator() {
      level=0;
      for(int i=0; i<LEVEL; i++) {
        iterators.add(priorityQueues.get(i).iterator());
      }
    }

    /**
     * Constrict an iterator for a single queue level
     * @param l the priority level to iterate over
     */
    private BlockIterator(int l) {
      level = l;
      isIteratorForLevel = true;
      iterators.add(priorityQueues.get(level).iterator());
    }

    private void update() {
      if (isIteratorForLevel) {
        return;
      }
      while(level< LEVEL-1 && !iterators.get(level).hasNext()) {
        level++;
      }
    }

    @Override
    public Block next() {
      if (isIteratorForLevel) {
        return iterators.get(0).next();
      }
      update();
      return iterators.get(level).next();
    }

    @Override
    public boolean hasNext() {
      if (isIteratorForLevel) {
        return iterators.get(0).hasNext();
      }
      update();
      return iterators.get(level).hasNext();
    }

    @Override
    public void remove() {
      if (isIteratorForLevel) {
        iterators.get(0).remove();
      } else {
        iterators.get(level).remove();
      }
    }

    int getPriority() {
      return level;
    }
  }  
}
