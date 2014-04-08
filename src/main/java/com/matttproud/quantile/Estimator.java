/*
 * Copyright 2013 Matt T. Proud (matt.proud@gmail.com) Copyright 2012 Andrew
 * Wang (andrew@umbrant.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.matttproud.quantile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * <p>
 * Implementation of the Cormode, Korn, Muthukrishnan, and Srivastava algorithm
 * for streaming calculation of targeted high-percentile epsilon-approximate
 * quantiles.
 * </p>
 *
 * <p>
 * This is a generalization of the earlier work by Greenwald and Khanna (GK),
 * which essentially allows different error bounds on the targeted quantiles,
 * which allows for far more efficient calculation of high-percentiles.
 * </p>
 *
 * <p>
 * See: Cormode, Korn, Muthukrishnan, and Srivastava
 * "Effective Computation of Biased Quantiles over Data Streams" in ICDE 2005
 * </p>
 *
 * <p>
 * Greenwald and Khanna,
 * "Space-efficient online computation of quantile summaries" in SIGMOD 2001
 * </p>
 *
 * <p>
 * This type is <em>not</em> concurrency safe.
 * </p>
 */
public class Estimator<T extends Number & Comparable<T>> {
  private static final int DEFAULT_BUFFER_CAPACITY = 4096;
  private int bufferCap;

  // Total number of items in stream
  int count = 0;

  /**
   * Current list of sampled items, maintained in sorted order with error bounds
   */
  final LinkedList<Item> samples = new LinkedList<Item>();
  /**
   * Buffers incoming items to be inserted in batch.
   */
  final ArrayList<T> buffer;
  /**
   * Array of Quantiles that we care about, along with desired error.
   */
  final Quantile quantiles[];

  /**
   * Create an estimator with the provided invariants for quantile estimation.
   *
   * @param quantiles requested quantile invariants.
   */
  public Estimator(final Quantile... quantiles) {
    this.quantiles = quantiles;

    bufferCap = DEFAULT_BUFFER_CAPACITY;
    buffer = new ArrayList<T>(bufferCap);
  }

    /**
     * <p>
     * Create an estimator with default invariants for quantile estimation.
     * </p>
     *
     * <ul>
     * <li>Median at 5 percent inaccuracy.</li>
     * <li>99th Percentile at 0.1 percent inaccuracy.</li>
     * </ul>
     */
    public Estimator() {
        this(new Quantile(0.5, 0.05), new Quantile(0.99, 0.001));
    }

    /**
     * <p>
     * Create an estimator with default invariants for quantile estimation.
     * </p>
     *
     * <ul>
     * <li>Median at 5 percent inaccuracy.</li>
     * <li>99th Percentile at 0.1 percent inaccuracy.</li>
     * </ul>
     *
     * @param bufferCap The capacity for the internal observations buffer.
     */
    public Estimator(final int bufferCap) {
        this(bufferCap, new Quantile(0.5, 0.05), new Quantile(0.99, 0.001));
    }

    /**
     * <p>
     * Create an estimator with the provided invariants for quantile estimation.
     * </p>
     *
     * @param quantiles requested quantile invariants.
     * @param bufferCap The capacity for the internal observations buffer.
     */
    public Estimator(final int bufferCap, final Quantile... quantiles) {
        this.quantiles = quantiles;

        this.bufferCap = bufferCap;
        buffer = new ArrayList<T>(bufferCap);
    }

  /**
   * <p>
   * Specifies the allowable error for this rank, depending on which quantiles
   * are being targeted.
   * </p>
   *
   * <p>
   * This is the f(r_i, n) function from the CKMS paper. It's basically how wide
   * the range of this rank can be.
   * </p>
   *
   * @param rank the index in the list of samples
   */
  private double allowableError(final int rank, final int n) {
    double minError = n + 1;
    for (final Quantile q : quantiles) {
      final double delta = q.delta(rank, n);
      if (delta < minError) {
        minError = delta;
      }
    }
    return Math.floor(minError);
  }

  /**
   * <p>
   * Add a new value from the stream.
   * </p>
   *
   * @param v observation to be added.
   */
  public void insert(final T v) {
    buffer.add(buffer.size(), v);

    if (buffer.size() == bufferCap) {
      flush();
    }
  }

  /**
   * <p>
   * Add new values from the stream.
   * </p>
   *
   * @param vs observations to be added.
   */
  public void insert(final Collection<T> vs) {
    buffer.addAll(vs);

    if (buffer.size() >= bufferCap) {
      flush();
    }
  }

  private void mergeBuffer() {
    final int bufSize = buffer.size();

    if (bufSize == 0) {
      return;
    }

    Collections.sort(buffer);

    // Base case: no samples
    int start = 0;
    if (samples.size() == 0) {
      final Item newItem = new Item(buffer.get(0), 1, 0);
      samples.add(newItem);
      start++;
      count++;
    }

    final ListIterator<Item> it = samples.listIterator();
    Item item = it.next();
    for (int i = start; i < bufSize; i++) {
      final T v = buffer.get(i);
      while (it.nextIndex() < samples.size() && item.value.compareTo(v) < 0) {
        item = it.next();
      }
      // If we found that bigger item, back up so we insert ourselves before it
      if (item.value.compareTo(v) > 0) {
        it.previous();
      }
      // We use different indexes for the edge comparisons, because of the above
      // if statement that adjusts the iterator
      int delta;
      if (it.previousIndex() == 0 || it.nextIndex() == samples.size()) {
        delta = 0;
      } else {
        delta = ((int) Math.floor(allowableError(it.nextIndex(), samples.size()))) - 1;
      }
      final Item newItem = new Item(v, 1, delta);
      it.add(newItem);
      count++;
      item = newItem;
    }

    // Return the buffer back to its originally allocated size in case the
    // addition overshot the range to be a good memory citizen.
    if (bufSize > bufferCap) {
      buffer.subList(bufferCap, bufSize).clear();
      buffer.trimToSize();
    }

    buffer.clear();
  }

  /**
   * Try to remove extraneous items from the set of sampled items. This checks
   * if an item is unnecessary based on the desired error bounds, and merges it
   * with the adjacent item if it is.
   */
  private void compress() {
    if (samples.size() < 2) {
      return;
    }

    final ListIterator<Item> it = samples.listIterator();

    Item prev = null;
    Item next = it.next();
    while (it.hasNext()) {
      prev = next;
      next = it.next();

      if (prev.g + next.g + next.delta <= allowableError(it.previousIndex(), samples.size())) {
        next.g += prev.g;
        // Remove prev. it.remove() kills the last thing returned.
        it.previous();
        it.previous();
        it.remove();
        // it.next() is now equal to next, skip it back forward again
        it.next();
      }
    }
  }

  /**
   * Get the estimated value at the specified quantile.
   *
   * @param quantile Queried quantile, e.g. 0.50 or 0.99.
   * @return Estimated value at that quantile.
   */
  public T query(final double quantile) throws IllegalStateException {
    flush();

    if (samples.size() == 0) {
      throw new IllegalStateException("No samples present");
    }

    int rankMin = 0;
    final int desired = (int) (quantile * count);

    final ListIterator<Item> it = samples.listIterator();
    Item prev, cur;
    cur = it.next();
    while (it.hasNext()) {
      prev = cur;
      cur = it.next();

      rankMin += prev.g;

      if (rankMin + cur.g + cur.delta > desired + (allowableError(desired, samples.size()) / 2)) {
        return prev.value;
      }
    }

    // edge case of wanting max value
    return samples.getLast().value;
  }

  void flush() {
    mergeBuffer();
    compress();
  }

  private class Item {
    final T value;
    int g;
    final int delta;

    public Item(final T value, final int lowerDelta, final int delta) {
      this.value = value;
      this.g = lowerDelta;
      this.delta = delta;
    }

    @Override
    public String toString() {
      return String.format("%d, %d, %d", value, g, delta);
    }
  }
}
