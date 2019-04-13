package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.indexengine;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class OIndexEngineDeleteCO extends OComponentOperationRecord {
  private int indexId;

  private String engineName;
  private String algorithm;
  private String indexType;

  private byte keySerializerId;
  private byte valueSerializerId;

  private boolean             isAutomatic;
  private int                 version;
  private int                 apiVersion;
  private boolean             multiValue;
  private Map<String, String> engineProperties;

  private int     keySize;
  private OType[] keyTypes;

  private boolean nullValuesSupport;

  public OIndexEngineDeleteCO() {
  }

  public OIndexEngineDeleteCO(final int indexId, final String engineName, final String algorithm, final String indexType,
      final byte keySerializerId, final byte valueSerializerId, final boolean isAutomatic, final int version, final int apiVersion,
      final boolean multiValue, final Map<String, String> engineProperties, final int keySize, final OType[] keyTypes,
      final boolean nullValuesSupport) {
    this.indexId = indexId;
    this.engineName = engineName;
    this.algorithm = algorithm;
    this.indexType = indexType;
    this.keySerializerId = keySerializerId;
    this.valueSerializerId = valueSerializerId;
    this.isAutomatic = isAutomatic;
    this.version = version;
    this.apiVersion = apiVersion;
    this.multiValue = multiValue;
    this.engineProperties = engineProperties;
    this.keySize = keySize;
    this.keyTypes = keyTypes;
    this.nullValuesSupport = nullValuesSupport;
  }

  public int getIndexId() {
    return indexId;
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) throws IOException {
    storage.deleteIndexEngineInternal(indexId);
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) throws IOException {
    final OBinarySerializerFactory binarySerializerFactory = OBinarySerializerFactory.getInstance();
    storage.addIndexEngineInternal(engineName, algorithm, indexType, binarySerializerFactory.getObjectSerializer(valueSerializerId),
        isAutomatic, true, version, apiVersion, multiValue, engineProperties,
        binarySerializerFactory.getObjectSerializer(keySerializerId), keySize, keyTypes, nullValuesSupport);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(indexId);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    indexId = buffer.getInt();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.INDEX_ENGINE_DELETE_CO;
  }
}
