/*
   Copyright 2012 Andrew Wang (andrew@umbrant.com)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.umbrant.quantile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Implementation of the Cormode, Korn, Muthukrishnan, and Srivastava algorithm
 * for streaming calculation of targeted high-percentile epsilon-approximate
 * quantiles.
 * 
 * This is a generalization of the earlier work by Greenwald and Khanna (GK),
 * which essentially allows different error bounds on the targeted quantiles,
 * which allows for far more efficient calculation of high-percentiles.
 * 
 * 
 * See: Cormode, Korn, Muthukrishnan, and Srivastava
 * "Effective Computation of Biased Quantiles over Data Streams" in ICDE 2005
 * 
 * Greenwald and Khanna,
 * "Space-efficient online computation of quantile summaries" in SIGMOD 2001
 * 
 */
public class QuantileEstimationCKMS {

  private static Logger LOG = Logger.getLogger(QuantileEstimationCKMS.class);
  static {
    BasicConfigurator.configure();
    LOG.setLevel(Level.INFO);
  }

  // Total number of items in stream
  int count = 0;

  // Used for tracking incremental compression
  private int compressIdx = 0;

  /**
   * Current list of sampled items, maintained in sorted order with error bounds
   */
  LinkedList<Item> sample;
  /**
   * Buffers incoming items to be inserted in batch.
   */
  long[] buffer = new long[500];
  int bufferCount = 0;
  /**
   * Array of Quantiles that we care about, along with desired error.
   */
  final Quantile quantiles[];

  public QuantileEstimationCKMS(Quantile[] quantiles) {
    this.quantiles = quantiles;
    this.sample = new LinkedList<Item>();
  }

  /**
   * Specifies the allowable error for this rank, depending on which quantiles
   * are being targeted.
   * 
   * This is the f(r_i, n) function from the CKMS paper. It's basically how wide
   * the range of this rank can be.
   * 
   * @param rank
   *          the index in the list of samples
   */
  private double allowableError(int rank) {
    // NOTE: according to CKMS, this should be count, not size, but this leads
    // to error larger than the error bounds. Leaving it like this is
    // essentially a HACK, and blows up memory, but does "work".
    //int size = count;
    int size = sample.size();
    double minError = size + 1;
    for (Quantile q : quantiles) {
      double error;
      if (rank <= q.quantile * size) {
        error = q.u * (size - rank);
      } else {
        error = q.v * rank;
      }
      if (error < minError) {
        minError = error;
      }
    }

    return minError;
  }

  private void printList() {
    if (LOG.isDebugEnabled()) {
      StringBuffer buf = new StringBuffer("sample = ");
      for (Item i : sample) {
        buf.append(String.format("(%s),", i));
      }
      LOG.debug(buf.toString());
    }
  }

  private void printBuffer() {
    if (LOG.isDebugEnabled()) {
      StringBuffer buf = new StringBuffer("buffer = [");
      for (int i = 0; i < bufferCount; i++) {
        buf.append(buffer[i] + ", ");
      }
      buf.append("]");
      LOG.debug(buf.toString());
    }
  }

  /**
   * Add a new value from the stream.
   * 
   * @param v
   */
  public void insert(long v) {
    buffer[bufferCount] = v;
    bufferCount++;
    printBuffer();

    if (bufferCount == buffer.length) {
      insertBatch();
      compress();
    }
  }

  private void insertBatch() {
    LOG.debug("insertBatch called");

    if (bufferCount == 0) {
      return;
    }
    printList();

    Arrays.sort(buffer, 0, bufferCount);
    printBuffer();

    // Base case: no samples
    int start = 0;
    if (sample.size() == 0) {
      Item newItem = new Item(buffer[0], 1, 0);
      sample.add(newItem);
      start++;
      count++;
    }

    ListIterator<Item> it = sample.listIterator();
    Item item = it.next();
    for (int i = start; i < bufferCount; i++) {
      long v = buffer[i];
      while (it.nextIndex() < sample.size() && item.value < v) {
        item = it.next();
      }
      // If we found that bigger item, back up so we insert ourselves before it
      if (item.value > v) {
        it.previous();
      }
      // We use different indexes for the edge comparisons, because of the above
      // if statement that adjusts the iterator
      int delta;
      if (it.previousIndex() == 0 || it.nextIndex() == sample.size()) {
        delta = 0;
      } else {
        delta = ((int) Math.floor(allowableError(it.nextIndex()))) - 1;
      }
      Item newItem = new Item(v, 1, delta);
      it.add(newItem);
      count++;
      item = newItem;
      printList();
    }

    bufferCount = 0;
    printList();
    LOG.debug("insertBatch finished");
  }

  /**
   * Try to remove extraneous items from the set of sampled items. This checks
   * if an item is unnecessary based on the desired error bounds, and merges it
   * with the adjacent item if it is.
   */
  public void compress() {

    if (sample.size() < 2) {
      return;
    }

    ListIterator<Item> it = sample.listIterator();
    int removed = 0;

    Item prev = null;
    Item next = it.next();
    while (it.hasNext()) {
      prev = next;
      next = it.next();

      if (prev.g + next.g + next.delta <= allowableError(it.previousIndex())) {
        next.g += prev.g;
        // Remove prev. it.remove() kills the last thing returned.
        it.previous();
        it.previous();
        it.remove();
        // it.next() is now equal to next, skip it back forward again
        it.next();
        removed++;
      }
    }

    LOG.debug("Removed " + removed + " items");
  }

  /**
   * Get the estimated value at the specified quantile.
   * 
   * @param quantile
   *          Queried quantile, e.g. 0.50 or 0.99.
   * @return Estimated value at that quantile.
   */
  public long query(double quantile) throws IOException {

    // clear the buffer
    insertBatch();
    compress();

    if (sample.size() == 0) {
      throw new IOException("No samples present");
    }

    int rankMin = 0;
    int desired = (int) (quantile * count);

    ListIterator<Item> it = sample.listIterator();
    Item prev, cur;
    cur = it.next();
    while (it.hasNext()) {
      prev = cur;
      cur = it.next();

      rankMin += prev.g;

      if (rankMin + cur.g + cur.delta > desired + (allowableError(desired) / 2)) {
        return prev.value;
      }
    }

    // edge case of wanting max value
    return sample.getLast().value;
  }
}
