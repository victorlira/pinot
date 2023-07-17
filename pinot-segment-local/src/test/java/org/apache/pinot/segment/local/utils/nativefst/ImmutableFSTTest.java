/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.utils.nativefst;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTInfo;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Additional tests for {@link ImmutableFST}.
 */
public final class ImmutableFSTTest {
  public List<String> _expected = Arrays.asList("a", "aba", "ac", "b", "ba", "c");

  public static void walkNode(byte[] buffer, int depth, FST fst, int node, int cnt, List<String> result) {
    for (int arc = fst.getFirstArc(node); arc != 0; arc = fst.getNextArc(arc)) {
      buffer[depth] = fst.getArcLabel(arc);

      if (fst.isArcFinal(arc) || fst.isArcTerminal(arc)) {
        result.add(cnt + " " + new String(buffer, 0, depth + 1, UTF_8));
      }

      if (fst.isArcFinal(arc)) {
        cnt++;
      }

      if (!fst.isArcTerminal(arc)) {
        walkNode(buffer, depth + 1, fst, fst.getEndNode(arc), cnt, result);
        cnt += fst.getRightLanguageCount(fst.getEndNode(arc));
      }
    }
  }

  private static void verifyContent(FST fst, List<String> expected) {
    List<String> actual = new ArrayList<>();
    for (ByteBuffer bb : fst.getSequences()) {
      assertEquals(0, bb.arrayOffset());
      assertEquals(0, bb.position());
      actual.add(new String(bb.array(), 0, bb.remaining(), UTF_8));
    }
    actual.sort(null);
    assertEquals(actual, expected);
  }

  @Test
  public void testVersion5()
      throws IOException {
    try {
      ByteBuffer buffer =
          ByteBuffer.wrap(getClass().getClassLoader().getResourceAsStream("data/abc.native.fst").readAllBytes());
      FST fst = FST.read(buffer);
      assertFalse(fst.getFlags().contains(FSTFlags.NUMBERS));
      verifyContent(fst, _expected);
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testVersion5WithNumbers()
      throws IOException {
    try {
      ByteBuffer buffer = ByteBuffer.wrap(
          getClass().getClassLoader().getResourceAsStream("data/abc-numbers.native.fst").readAllBytes());
      FST fst = FST.read(buffer);
      assertTrue(fst.getFlags().contains(FSTFlags.NUMBERS));
      verifyContent(fst, _expected);
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testArcsAndNodes()
      throws IOException {
    for (String resourceName : new String[]{"data/abc.native.fst", "data/abc-numbers.native.fst"}) {
      try {
        ByteBuffer buffer =
            ByteBuffer.wrap(getClass().getClassLoader().getResourceAsStream(resourceName).readAllBytes());
        FST fst = FST.read(buffer);
        FSTInfo fstInfo = new FSTInfo(fst);
        assertEquals(fstInfo._nodeCount, 4);
        assertEquals(fstInfo._arcsCount, 7);
      } catch (IOException e) {
        throw e;
      }
    }
  }

  @Test
  public void testNumbers()
      throws IOException {
    try {
      ByteBuffer in = ByteBuffer.wrap(
          getClass().getClassLoader().getResourceAsStream("data/abc-numbers.native.fst").readAllBytes());
      FST fst = FST.read(in);
      assertTrue(fst.getFlags().contains(FSTFlags.NEXTBIT));

      // Get all numbers for nodes.
      byte[] buffer = new byte[128];
      List<String> result = new ArrayList<>();
      walkNode(buffer, 0, fst, fst.getRootNode(), 0, result);

      result.sort(null);
      assertEquals(result, Arrays.asList("0 c", "1 b", "2 ba", "3 a", "4 ac", "5 aba"));
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testSave()
      throws IOException {
    List<String> inputList = Arrays.asList("aeh", "pfh");

    FSTBuilder builder = new FSTBuilder();
    for (String input : inputList) {
      builder.add(input.getBytes(UTF_8), 0, input.length(), 127);
    }
    FST fst = builder.complete();

    File fstFile = new File(FileUtils.getTempDirectory(), "test.native.fst");
    fst.save(new FileOutputStream(fstFile));

    try {
      FileChannel fileChannel = (FileChannel) Files.newByteChannel(
          fstFile.toPath(), EnumSet.of(StandardOpenOption.READ));
      MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

      verifyContent(FST.read(mappedByteBuffer, ImmutableFST.class, true), inputList);
    } catch (IOException e) {
      throw e;
    }

    FileUtils.deleteQuietly(fstFile);
  }
}
