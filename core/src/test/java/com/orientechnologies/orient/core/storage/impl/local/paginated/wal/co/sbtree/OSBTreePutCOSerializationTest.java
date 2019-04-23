package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbtree;

import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

public class OSBTreePutCOSerializationTest {
  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final String value = "value";
    final byte valueSerializerId = OStringSerializer.ID;

    final int indexId = 12;

    final byte keySerializerId = 10;

    OSBTreePutCO co = new OSBTreePutCO(indexId, null, keySerializerId, null, valueSerializerId, value);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];
    int pos = co.toStream(stream, 1);

    Assert.assertEquals(size + 1, pos);

    OSBTreePutCO restoredCO = new OSBTreePutCO();
    pos = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(value, restoredCO.getValue());
    Assert.assertEquals(valueSerializerId, restoredCO.getValueSerializerId());
  }
}
