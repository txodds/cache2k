package org.cache2k.core.eviction;

/*
 * #%L
 * cache2k core implementation
 * %%
 * Copyright (C) 2000 - 2021 headissue GmbH, Munich
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.cache2k.Cache;
import org.cache2k.annotation.Nullable;
import org.cache2k.core.util.TunableFactory;
import org.cache2k.processor.EntryProcessor;
import org.cache2k.processor.MutableCacheEntry;
import org.cache2k.test.util.TestingBase;
import org.cache2k.testing.category.FastTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 * Run simple access patterns that provide test coverage on the clock pro
 * eviction.
 *
 * @author Jens Wilke
 */
@Category(FastTests.class)
public class ClockProEvictionPreserveNonExpiredTest extends TestingBase {

  protected Cache<Integer, Integer> provideCache(long size) {
    return builder(Integer.class, Integer.class).eternal(true).entryCapacity(size).preserveNonExpired(true).build();
  }

  protected Cache<Integer, Integer> provideCacheExp(long size) {
    return builder(Integer.class, Integer.class).entryCapacity(size).expireAfterWrite(1, TimeUnit.MILLISECONDS)
        .sharpExpiry(false).keepDataAfterExpired(true).preserveNonExpired(true).build();
  }

  @Test
  public void testChunking() {
    final int maxSize = 10000;
    Cache<Integer, Integer> c = provideCache(maxSize);
    int evictionChunk = 1;
    int previousSize = 0;
    for (int i = 0; i < maxSize * 2; i++) {
      c.put(i, 1);
      int size = c.asMap().size();
      assertEquals("no items have been evicted", size, previousSize + 1);
      previousSize = size;
    }
  }

  @Test
  public void test1() {
    final int size = 1;
    Cache<Integer, Integer> c = provideCache(size);
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
    }
    int count = 0;
    for (int k : c.keys()) {
      count++;
    }
    assertEquals(size * 2, count);
  }

  @Test
  public void test30() {
    final int size = 30;
    Cache<Integer, Integer> c = provideCache(size);
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
    }
    int count = 0;
    for (int k : c.keys()) {
      count++;
    }
    assertEquals(size * 2, count);
  }

  @Test
  public void testEvictCold() {
    final int size = 30;
    Cache<Integer, Integer> c = provideCache(size);
    for (int i = 0; i < size / 2; i++) {
      c.put(i, i);
    }
    for (int i = 0; i < size / 2; i++) {
      c.put(i, i);
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
    }
    int count = 0;
    for (int k : c.keys()) {
      count++;
    }
    assertEquals(size * 2, count);
  }

  @Test
  public void testEvictHot() {
    final int size = 30;
    Cache<Integer, Integer> c = provideCache(size);
    for (int i = 0; i < size / 2; i++) {
      c.put(i, i);
    }
    for (int i = 0; i < size / 2; i++) {
      c.put(i, i);
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
    }
    for (int i = size / 2; i < size; i++) {
      c.put(i, i);
    }
    for (int i = size / 2; i < size; i++) {
      c.put(i, i);
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
    }
    int count = 0;
    for (int k : c.keys()) {
      count++;
    }
    assertEquals(size * 2, count);
  }

  /**
   * Additional test to extend test coverage
   */
  @Test
  public void testEvictHot2() {
    final int size = 30;
    Cache<Integer, Integer> c = provideCache(size);
    for (int i = 0; i < size / 2; i++) {
      c.put(i, i);
    }
    for (int i = 0; i < size / 2; i++) {
      c.put(i, i);
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
    }
    assertEquals(size * 2, countEntriesViaIteration());
    for (int i = size / 2; i < size; i++) {
      c.put(i, i);
    }
    for (int i = size / 2; i < size; i++) {
      c.put(i, i);
    }
    for (int i = 0; i < size / 3; i++) {
      c.put(i, i);
    }
    int hitCounterDecreaseShift = TunableFactory.get(ClockProPlusEviction.Tunable.class).hitCounterDecreaseShift;
    for (int j = 0; j < 1 << hitCounterDecreaseShift + 1; j++) {
      for (int i = 0; i < size / 4; i++) {
        c.put(i, i);
      }
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
    }
    assertEquals(size * 2, countEntriesViaIteration());
  }

  @Test
  public void testChunkingExpired() {
    final int maxSize = 10000;
    final int minChunkSize = 1;
    Cache<Integer, Integer> c = provideCacheExp(maxSize);
    int evictionChunk = 1;
    int previousSize = 0;
    for (int i = 0; i < maxSize * 2; i++) {
      c.put(i, 1);
      int size = c.asMap().size();
      if (size < previousSize + 1) {
        evictionChunk = Math.max((previousSize + 1) - size, evictionChunk);
      }
      if (evictionChunk > minChunkSize) {
        break;
      }
      previousSize = size;
    }
    assertThat("chunked eviction happened", evictionChunk, greaterThan(minChunkSize));
  }

  private void smallDelay() {
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
    }
  }

  @Test
  public void test1Expired() {
    final int size = 1;
    Cache<Integer, Integer> c = provideCacheExp(size);
    for (int i = 0; i < size; i++) {
      final int ii = i;
      c.invoke(size*100 + i, new EntryProcessor<Integer, Integer, Boolean>() {
        @Override
        public @Nullable Boolean process(MutableCacheEntry<Integer, Integer> entry) throws Exception {
          entry.setValue(ii);
          entry.setExpiryTime(System.currentTimeMillis() + 3600000);
          return true;
        }
      });
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    int count = 0;
    for (int k : c.keys()) {
      count++;
    }
    assertEquals(size, count);
  }

  @Test
  public void test30Expired() {
    final int size = 30;
    Cache<Integer, Integer> c = provideCacheExp(size);
    for (int i = 0; i < size; i++) {
      final int ii = i;
      c.invoke(size*100 + i, new EntryProcessor<Integer, Integer, Boolean>() {
        @Override
        public @Nullable Boolean process(MutableCacheEntry<Integer, Integer> entry) throws Exception {
          entry.setValue(ii);
          entry.setExpiryTime(System.currentTimeMillis() + 3600000);
          return true;
        }
      });
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    int count = 0;
    for (int k : c.keys()) {
      count++;
    }
    assertEquals(size, count);
  }

  @Test
  public void testEvictColdExpired() {
    final int size = 30;
    Cache<Integer, Integer> c = provideCacheExp(size);
    for (int i = 0; i < size; i++) {
      final int ii = i;
      c.invoke(size*100 + i, new EntryProcessor<Integer, Integer, Boolean>() {
        @Override
        public @Nullable Boolean process(MutableCacheEntry<Integer, Integer> entry) throws Exception {
          entry.setValue(ii);
          entry.setExpiryTime(System.currentTimeMillis() + 3600000);
          return true;
        }
      });
    }
    for (int i = 0; i < size / 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = 0; i < size / 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    int count = 0;
    for (int k : c.keys()) {
      count++;
    }
    assertEquals(size, count);
  }

  @Test
  public void testEvictHotExpired() {
    final int size = 30;
    Cache<Integer, Integer> c = provideCacheExp(size);
    for (int i = 0; i < size; i++) {
      final int ii = i;
      c.invoke(size*100 + i, new EntryProcessor<Integer, Integer, Boolean>() {
        @Override
        public @Nullable Boolean process(MutableCacheEntry<Integer, Integer> entry) throws Exception {
          entry.setValue(ii);
          entry.setExpiryTime(System.currentTimeMillis() + 3600000);
          return true;
        }
      });
    }
    for (int i = 0; i < size / 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = 0; i < size / 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = size / 2; i < size; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = size / 2; i < size; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    int count = 0;
    for (int k : c.keys()) {
      count++;
    }
    assertEquals(size, count);
  }

  /**
   * Additional test to extend test coverage
   */
  @Test
  public void testEvictHot2Expired() {
    final int size = 30;
    Cache<Integer, Integer> c = provideCacheExp(size);
    for (int i = 0; i < size; i++) {
      final int ii = i;
      c.invoke(size*100 + i, new EntryProcessor<Integer, Integer, Boolean>() {
        @Override
        public @Nullable Boolean process(MutableCacheEntry<Integer, Integer> entry) throws Exception {
          entry.setValue(ii);
          entry.setExpiryTime(System.currentTimeMillis() + 3600000);
          return true;
        }
      });
    }
    for (int i = 0; i < size / 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = 0; i < size / 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    assertEquals(size, countEntriesViaIteration());
    for (int i = size / 2; i < size; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = size / 2; i < size; i++) {
      c.put(i, i);
      smallDelay();
    }
    for (int i = 0; i < size / 3; i++) {
      c.put(i, i);
      smallDelay();
    }
    int hitCounterDecreaseShift = TunableFactory.get(ClockProPlusEviction.Tunable.class).hitCounterDecreaseShift;
    for (int j = 0; j < 1 << hitCounterDecreaseShift + 1; j++) {
      for (int i = 0; i < size / 4; i++) {
        c.put(i, i);
      smallDelay();
      }
    }
    for (int i = 0; i < size * 2; i++) {
      c.put(i, i);
      smallDelay();
    }
    assertEquals(size, countEntriesViaIteration());
  }
}
