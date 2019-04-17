package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreemultivaluev2;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class OCellBTreeMultiValueV2PutCOSerializationTest {
  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final String encryptionName = "encryption";
    final byte keySerializerId = 12;
    final int indexId = 456;

    final Random random = new Random();
    final byte[] key = new byte[12];
    random.nextBytes(key);

    final byte[] value = new byte[23];
    random.nextBytes(value);

    OCellBTreeMultiValueV2PutCO co = new OCellBTreeMultiValueV2PutCO(encryptionName, keySerializerId, indexId, key, value);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];

    int pos = co.toStream(stream, 1);
    Assert.assertEquals(size + 1, pos);

    OCellBTreeMultiValueV2PutCO restoredCO = new OCellBTreeMultiValueV2PutCO();
    pos = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredCO.getOperationUnitId());
    Assert.assertEquals(encryptionName, restoredCO.getEncryptionName());
    Assert.assertEquals(keySerializerId, restoredCO.getKeySerializerId());
    Assert.assertEquals(indexId, restoredCO.getIndexId());

    Assert.assertArrayEquals(key, restoredCO.getKey());
    Assert.assertArrayEquals(value, restoredCO.getValue());
  }

  @Test
  public void testNullSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final String encryptionName = null;
    final byte keySerializerId = 12;
    final int indexId = 456;

    final Random random = new Random();
    final byte[] key = null;

    final byte[] value = new byte[23];
    random.nextBytes(value);

    OCellBTreeMultiValueV2PutCO co = new OCellBTreeMultiValueV2PutCO(encryptionName, keySerializerId, indexId, key, value);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];

    int pos = co.toStream(stream, 1);
    Assert.assertEquals(size + 1, pos);

    OCellBTreeMultiValueV2PutCO restoredCO = new OCellBTreeMultiValueV2PutCO();
    pos = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredCO.getOperationUnitId());
    Assert.assertEquals(encryptionName, restoredCO.getEncryptionName());
    Assert.assertEquals(keySerializerId, restoredCO.getKeySerializerId());
    Assert.assertEquals(indexId, restoredCO.getIndexId());

    Assert.assertArrayEquals(key, restoredCO.getKey());
    Assert.assertArrayEquals(value, restoredCO.getValue());
  }
}
