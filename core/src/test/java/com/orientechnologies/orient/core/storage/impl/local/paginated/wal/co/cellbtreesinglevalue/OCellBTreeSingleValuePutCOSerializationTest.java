package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreesinglevalue;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class OCellBTreeSingleValuePutCOSerializationTest {
  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final Random random = new Random();

    final byte[] key = new byte[12];
    random.nextBytes(key);

    final byte[] value = new byte[34];
    random.nextBytes(value);

    final byte[] oldValue = new byte[23];
    random.nextBytes(oldValue);

    final byte keySerializerId = 12;

    final int indexId = 45;
    final String encryptionName = "encryption";

    OCellBTreeSingleValuePutCO co = new OCellBTreeSingleValuePutCO(key, value, oldValue, keySerializerId, indexId, encryptionName);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];

    int position = co.toStream(stream, 1);
    Assert.assertEquals(size + 1, position);

    OCellBTreeSingleValuePutCO restoredCO = new OCellBTreeSingleValuePutCO();
    position = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, position);

    Assert.assertEquals(operationUnitId, restoredCO.getOperationUnitId());

    Assert.assertArrayEquals(key, restoredCO.getKey());
    Assert.assertArrayEquals(value, restoredCO.getValue());
    Assert.assertEquals(keySerializerId, restoredCO.getKeySerializerId());
    Assert.assertEquals(indexId, restoredCO.getIndexId());
    Assert.assertEquals(encryptionName, restoredCO.getEncryptionName());
  }

  @Test
  public void testNullSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final Random random = new Random();

    final byte[] key = null;

    final byte[] value = new byte[34];
    random.nextBytes(value);

    final byte[] oldValue = null;

    final byte keySerializerId = 12;

    final int indexId = 45;
    final String encryptionName = null;

    OCellBTreeSingleValuePutCO co = new OCellBTreeSingleValuePutCO(key, value, oldValue, keySerializerId, indexId, encryptionName);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];

    int position = co.toStream(stream, 1);
    Assert.assertEquals(size + 1, position);

    OCellBTreeSingleValuePutCO restoredCO = new OCellBTreeSingleValuePutCO();
    position = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, position);

    Assert.assertEquals(operationUnitId, restoredCO.getOperationUnitId());

    Assert.assertArrayEquals(key, restoredCO.getKey());
    Assert.assertArrayEquals(value, restoredCO.getValue());
    Assert.assertEquals(keySerializerId, restoredCO.getKeySerializerId());
    Assert.assertEquals(indexId, restoredCO.getIndexId());
    Assert.assertEquals(encryptionName, restoredCO.getEncryptionName());
  }
}
