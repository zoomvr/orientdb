package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.distributed;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class DistributedTxInfoWALRecordTest {

  @Test
  public void testSerialization() {

    byte[] content = new byte[]{1, 2, 3, 4, 5};
    DistributedTxInfoWALRecord operation = new DistributedTxInfoWALRecord(content);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    DistributedTxInfoWALRecord restoredOperation = new DistributedTxInfoWALRecord();
    restoredOperation.fromStream(stream, 1);

    Assert.assertTrue(Arrays.equals(content, restoredOperation.getContent()));
  }

}
