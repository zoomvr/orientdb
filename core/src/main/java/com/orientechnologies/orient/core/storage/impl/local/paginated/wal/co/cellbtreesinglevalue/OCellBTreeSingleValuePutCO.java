package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreesinglevalue;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OCellBTreeSingleValuePutCO extends OComponentOperationRecord {
  private byte[] key;
  private byte[] value;

  private byte[] oldValue;

  private byte keySerializerId;

  private int    indexId;
  private String encryptionName;

  public OCellBTreeSingleValuePutCO() {
  }

  public OCellBTreeSingleValuePutCO(final byte[] key, final byte[] value, final byte[] oldValue, final byte keySerializerId,
      final int indexId, final String encryptionName) {
    this.key = key;
    this.value = value;
    this.oldValue = oldValue;
    this.keySerializerId = keySerializerId;
    this.indexId = indexId;
    this.encryptionName = encryptionName;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  public byte getKeySerializerId() {
    return keySerializerId;
  }

  public String getEncryptionName() {
    return encryptionName;
  }

  public int getIndexId() {
    return indexId;
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) throws IOException {
    final Object deserializedKey = deserializeKey(storage);

    final short clusterId = OShortSerializer.INSTANCE.deserializeNative(value, 0);
    final long clusterPosition = OLongSerializer.INSTANCE.deserializeNative(value, OShortSerializer.SHORT_SIZE);

    try {
      storage.putRidIndexEntryInternal(indexId, deserializedKey, new ORecordId(clusterId, clusterPosition));
    } catch (OInvalidIndexEngineIdException e) {
      throw OException.wrapException(new OStorageException("Can not redo operation for index with id " + indexId), e);
    }
  }

  private Object deserializeKey(final OAbstractPaginatedStorage storage) {

    final OBinarySerializerFactory binarySerializerFactory = OBinarySerializerFactory.getInstance();
    final OBinarySerializer keySerializer = binarySerializerFactory.getObjectSerializer(keySerializerId);

    if (key == null) {
      return null;
    }

    if (encryptionName != null) {
      final OEncryptionFactory encryptionFactory = OEncryptionFactory.INSTANCE;
      final OStorageConfiguration storageConfiguration = storage.getConfiguration();
      final String encryptionKey = storageConfiguration.getContextConfiguration().
          getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);

      final OEncryption encryption = encryptionFactory.getEncryption(encryptionName, encryptionKey);
      final byte[] decryptedKey = encryption.decrypt(key, OIntegerSerializer.INT_SIZE, key.length - OIntegerSerializer.INT_SIZE);

      return keySerializer.deserializeNativeObject(decryptedKey, 0);
    }

    return keySerializer.deserializeNativeObject(key, 0);
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) throws IOException {
    final Object deserializedKey = deserializeKey(storage);

    try {
      if (oldValue == null) {
        storage.removeKeyFromIndexInternal(indexId, deserializedKey);
      } else {
        final short clusterId = OShortSerializer.INSTANCE.deserializeNative(oldValue, 0);
        final long clusterPosition = OLongSerializer.INSTANCE.deserializeNative(oldValue, OShortSerializer.SHORT_SIZE);

        storage.putRidIndexEntryInternal(indexId, deserializedKey, new ORecordId(clusterId, clusterPosition));
      }
    } catch (OInvalidIndexEngineIdException e) {
      throw OException.wrapException(new OStorageException("Can not undo operation for index with id " + indexId), e);
    }
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    if (key == null) {
      buffer.putInt(0);
    } else {
      buffer.putInt(key.length);
      buffer.put(key);
    }

    buffer.putInt(value.length);
    buffer.put(value);

    buffer.put(keySerializerId);

    buffer.putInt(indexId);

    if (encryptionName == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);
      OStringSerializer.INSTANCE.serializeInByteBufferObject(encryptionName, buffer);
    }
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    final int keyLen = buffer.getInt();

    if (keyLen != 0) {
      key = new byte[keyLen];

      buffer.get(key);
    } else {
      key = null;
    }

    final int valueLen = buffer.getInt();
    value = new byte[valueLen];
    buffer.get(value);

    keySerializerId = buffer.get();

    indexId = buffer.getInt();

    if (buffer.get() == 1) {
      encryptionName = OStringSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);
    }
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CELL_BTREE_SINGLE_VALUE_PUT_CO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OByteSerializer.BYTE_SIZE + 3 * OIntegerSerializer.INT_SIZE + (key != null ? key.length : 0)
        + value.length + OByteSerializer.BYTE_SIZE + (encryptionName != null ?
        OStringSerializer.INSTANCE.getObjectSize(encryptionName) :
        0);
  }
}
