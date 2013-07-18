/*
 * Copyright 2012 Andrew Wang (andrew@umbrant.com)
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

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.*;

public class TestEstimator {
  private static Logger LOG = Logger.getLogger(Estimator.class);
  static {
    BasicConfigurator.configure();
    LOG.setLevel(Level.INFO);
  }

  @Test
  public void TestEstimator() {

    final int window_size = 1000000;
    boolean generator = false;

    List<Quantile> quantiles = new ArrayList<Quantile>();
    quantiles.add(new Quantile(0.50, 0.050));
    quantiles.add(new Quantile(0.90, 0.010));
    quantiles.add(new Quantile(0.95, 0.005));
    quantiles.add(new Quantile(0.99, 0.001));

    Estimator<Long> estimator = new Estimator<Long>(quantiles.toArray(new Quantile[] {}));

    LOG.info("Inserting into estimator...");

    long insertBegin = System.currentTimeMillis();
    Random rand = new Random(0xDEADBEEF);

    if (generator) {
      for (int i = 0; i < window_size; i++) {
        estimator.insert(rand.nextLong());
      }
    } else {
      Long[] shuffle = new Long[window_size];
      for (int i = 0; i < shuffle.length; i++) {
        shuffle[i] = (long) i;
      }
      Collections.shuffle(Arrays.asList(shuffle), rand);
      for (long l : shuffle) {
        estimator.insert(l);
      }
    }

    long insertEnd = System.currentTimeMillis();

    for (Quantile quantile : quantiles) {
      double q = quantile.quantile;
      long estimate = estimator.query(q);
      long actual = (long) ((q) * (window_size - 1));
      double off = ((double) Math.abs(actual - estimate)) / (double) window_size;
      LOG.info(String.format("Q(%.2f, %.3f) was %d (off by %.3f)", quantile.quantile,
          quantile.error, estimate, off));
    }
    LOG.info("# of samples: " + estimator.samples.size());
    LOG.info("Insert Time (ms): " + (insertEnd - insertBegin) + " per item "
        + (float) (insertEnd - insertBegin) / (float) window_size);
    LOG.info("Extract Time (ms): " + (System.currentTimeMillis() - insertEnd));
  }
}
