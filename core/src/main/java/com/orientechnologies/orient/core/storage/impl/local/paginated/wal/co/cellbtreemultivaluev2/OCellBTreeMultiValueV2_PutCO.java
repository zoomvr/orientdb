package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreemultivaluev2;

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

import java.nio.ByteBuffer;

public class OCellBTreeMultiValueV2_PutCO extends OComponentOperationRecord {
  private byte keySerializerId;
  private int  indexId;

  private String encryptionName;

  private byte[] key;
  private byte[] value;

  public OCellBTreeMultiValueV2_PutCO() {
  }

  public OCellBTreeMultiValueV2_PutCO(final String encryptionName, final byte keySerializerId, final int indexId, final byte[] key,
      final byte[] value) {
    this.keySerializerId = keySerializerId;
    this.indexId = indexId;
    this.key = key;
    this.value = value;
    this.encryptionName = encryptionName;
  }

  public byte getKeySerializerId() {
    return keySerializerId;
  }

  public int getIndexId() {
    return indexId;
  }

  public String getEncryptionName() {
    return encryptionName;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) {
    final Object deserializedKey = deserializeKey(storage);

    final int clusterId = OShortSerializer.INSTANCE.deserializeNative(value, 0);
    final long clusterPosition = OLongSerializer.INSTANCE.deserializeNative(value, OShortSerializer.SHORT_SIZE);

    try {
      storage.putRidIndexEntryInternal(indexId, deserializedKey, new ORecordId(clusterId, clusterPosition));
    } catch (OInvalidIndexEngineIdException e) {
      throw OException.wrapException(new OStorageException("Can not redo operation for index with id " + indexId), e);
    }
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) {
    final Object deserializedKey = deserializeKey(storage);

    final int clusterId = OShortSerializer.INSTANCE.deserializeNative(value, 0);
    final long clusterPosition = OLongSerializer.INSTANCE.deserializeNative(value, OShortSerializer.SHORT_SIZE);

    try {
      storage.removeRidIndexEntryInternal(indexId, deserializedKey, new ORecordId(clusterId, clusterPosition));
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
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.put(keySerializerId);
    buffer.putInt(indexId);

    if (encryptionName != null) {
      buffer.put((byte) 1);

      OStringSerializer.INSTANCE.serializeInByteBufferObject(encryptionName, buffer);
    } else {
      buffer.put((byte) 0);
    }

    if (key == null) {
      buffer.putInt(0);
    } else {
      buffer.putInt(key.length);
      buffer.put(key);
    }

    buffer.putInt(value.length);
    buffer.put(value);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    keySerializerId = buffer.get();
    indexId = buffer.getInt();

    if (buffer.get() != 0) {
      encryptionName = OStringSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);
    }

    final int keyLen = buffer.getInt();
    if (keyLen > 0) {
      key = new byte[keyLen];
      buffer.get(key);
    }

    final int valueLen = buffer.getInt();
    value = new byte[valueLen];
    buffer.get(value);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CELL_BTREE_MULTI_VALUE_PUT_CO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OByteSerializer.BYTE_SIZE +  3 * OIntegerSerializer.INT_SIZE + (encryptionName != null ?
        OStringSerializer.INSTANCE.getObjectSize(encryptionName) :
        0) + (key != null ? key.length : 0) + value.length;
  }
}
