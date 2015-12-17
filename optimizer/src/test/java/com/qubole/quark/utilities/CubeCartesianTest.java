/*
 * Copyright (c) 2015. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.qubole.quark.utilities;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.qubole.quark.planner.QuarkCube;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static junit.framework.Assert.assertEquals;

/**
 * Created by amargoor on 9/3/15.
 */
public class CubeCartesianTest {

  public <E> void check(Set<Set<E>> res, int fLen, int sLen, Set<E> fSet, Set<E> sSet) {
    final Object[] objects = res.toArray();
    final Set<Integer> o1 = ((Set) objects[0]);
    final Set<Integer> o2 = ((Set) objects[1]);
    assertEquals("Sum of Cardenality of cartesian product is wrong", fLen + sLen,
        (o1.size() + o2.size()));
    if (o1.size() == fLen) {
      assertEquals(true, o1.containsAll(fSet));
      assertEquals(true, o2.containsAll(sSet));
    } else {
      assertEquals(true, o2.containsAll(fSet));
      assertEquals(true, o1.containsAll(sSet));
    }
  }

  @Test
  public void testCartesian1() {
    final Set<HashSet<Integer>> e1 =
        Sets.newHashSet(Sets.newHashSet(1, 2, 3), Sets.newHashSet(4));
    final Set<Set<Integer>> e2 = Sets.newHashSet();
    e2.add(Sets.newHashSet(5));
    final List<Set<Set<Integer>>> cartesianList = (List) ImmutableList.of(e1, e2);
    final Set<Set<Integer>> cartesianResult = QuarkCube.cartesian(cartesianList);
    assertEquals("Cardenality of cartesian product is wrong", 2, cartesianResult.size());
    check(cartesianResult, 4, 2, Sets.newHashSet(1, 2, 3, 5), Sets.newHashSet(4, 5));
  }

  @Test
  public void testCartesian2() {
    final Set<Set<Integer>> e1 =
        Sets.<Set<Integer>>newHashSet(Sets.newHashSet(1, 2));
    final Set<HashSet<Integer>> e2 =
        Sets.newHashSet(Sets.newHashSet(3), Sets.<Integer>newHashSet());
    final List<Set<Set<Integer>>> cartesianList = (List) ImmutableList.of(e1, e2);
    final Set<Set<Integer>> cartesianResult = QuarkCube.cartesian(cartesianList);
    assertEquals("Cardenality of cartesian product is wrong", 2, cartesianResult.size());
    check(cartesianResult, 2, 3, Sets.newHashSet(1, 2), Sets.newHashSet(1, 2, 3));
  }
}
