package com.orientechnologies.orient.core.fetch.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Set;
import java.util.Stack;

public class OJacksonFetchContext implements OFetchContext {
  protected final JsonGenerator                        jsonGenerator;
  protected final ORecordSerializerJSON.FormatSettings settings;
  protected final Stack<StringBuilder>                 typesStack      = new Stack<StringBuilder>();
  protected final Stack<ODocument>                     collectionStack = new Stack<ODocument>();

  public OJacksonFetchContext(final JsonGenerator jsonGenerator, final ORecordSerializerJSON.FormatSettings iSettings) {
    this.jsonGenerator = jsonGenerator;
    settings = iSettings;
  }

  public void onBeforeFetch(final ODocument iRootRecord) {
    typesStack.add(new StringBuilder());
  }

  public void onAfterFetch(final ODocument iRootRecord) {
    StringBuilder buffer = typesStack.pop();
    if (settings.keepTypes && buffer.length() > 0)
      try {
        jsonGenerator.writeStringField(ORecordSerializerJSON.ATTRIBUTE_FIELD_TYPES, buffer.toString());
      } catch (IOException e) {
        throw OException.wrapException(new OFetchException("Error writing field types"), e);
      }
  }

  public void onBeforeStandardField(final Object iFieldValue, final String iFieldName, final Object iUserObject, OType fieldType) {
    manageTypes(iFieldName, iFieldValue, fieldType);
  }

  public void onAfterStandardField(Object iFieldValue, String iFieldName, Object iUserObject, OType fieldType) {
  }

  public void onBeforeArray(final ODocument iRootRecord, final String iFieldName, final Object iUserObject,
      final OIdentifiable[] iArray) {
    onBeforeCollection(iRootRecord, iFieldName, iUserObject, null);
  }

  public void onAfterArray(final ODocument iRootRecord, final String iFieldName, final Object iUserObject) {
    onAfterCollection(iRootRecord, iFieldName, iUserObject);
  }

  public void onBeforeCollection(final ODocument iRootRecord, final String fieldName, final Object iUserObject,
      final Iterable<?> iterable) {
    try {
      manageTypes(fieldName, iterable, null);
      jsonGenerator.writeArrayFieldStart(fieldName);
      collectionStack.add(iRootRecord);
    } catch (IOException e) {
      throw OException.wrapException(
          new OFetchException("Error writing collection field " + fieldName + " of record " + iRootRecord.getIdentity()), e);
    }
  }

  public void onAfterCollection(final ODocument iRootRecord, final String iFieldName, final Object iUserObject) {
    try {
      jsonGenerator.writeEndArray();
      collectionStack.pop();
    } catch (IOException e) {
      throw OException.wrapException(
          new OFetchException("Error writing collection field " + iFieldName + " of record " + iRootRecord.getIdentity()), e);
    }
  }

  public void onBeforeMap(final ODocument iRootRecord, final String iFieldName, final Object iUserObject) {
    try {
      jsonGenerator.writeObjectFieldStart(iFieldName);
      if (!(iUserObject instanceof ODocument)) {
        collectionStack.add(new ODocument()); // <-- sorry for this... fixes #2845 but this mess should be rewritten...
      }
    } catch (IOException e) {
      throw OException
          .wrapException(new OFetchException("Error writing map field " + iFieldName + " of record " + iRootRecord.getIdentity()),
              e);
    }
  }

  public void onAfterMap(final ODocument iRootRecord, final String iFieldName, final Object iUserObject) {
    try {
      jsonGenerator.writeEndObject();
      if (!(iUserObject instanceof ODocument)) {
        collectionStack.pop();
      }
    } catch (IOException e) {
      throw OException
          .wrapException(new OFetchException("Error writing map field " + iFieldName + " of record " + iRootRecord.getIdentity()),
              e);
    }
  }

  public void onBeforeDocument(final ODocument iRootRecord, final ODocument iDocument, final String iFieldName,
      final Object iUserObject) {
    try {
      final String fieldName;
      if (!collectionStack.isEmpty() && collectionStack.peek().equals(iRootRecord))
        fieldName = null;
      else
        fieldName = iFieldName;
      jsonGenerator.writeObjectFieldStart(fieldName);
      writeSignature(jsonGenerator, iDocument);
    } catch (IOException e) {
      throw OException
          .wrapException(new OFetchException("Error writing link field " + iFieldName + " of record " + iRootRecord.getIdentity()),
              e);
    }
  }

  public void onAfterDocument(final ODocument iRootRecord, final ODocument iDocument, final String iFieldName,
      final Object iUserObject) {
    try {
      jsonGenerator.writeEndObject();
    } catch (IOException e) {
      throw OException
          .wrapException(new OFetchException("Error writing link field " + iFieldName + " of record " + iRootRecord.getIdentity()),
              e);
    }
  }

  public void writeLinkedValue(final OIdentifiable iRecord, final String iFieldName) throws IOException {
    jsonGenerator.writeString(iRecord.getIdentity().toString());
  }

  public void writeLinkedAttribute(final OIdentifiable iRecord, final String iFieldName) throws IOException {
    final String link = iRecord.getIdentity().isValid() ? iRecord.getIdentity().toString() : null;
    jsonGenerator.writeStringField(iFieldName, link);
  }

  public boolean isInCollection(ODocument record) {
    return !collectionStack.isEmpty() && collectionStack.peek().equals(record);
  }

  public JsonGenerator getJsonGenerator() {
    return jsonGenerator;
  }

  public int getIndentLevel() {
    return settings.indentLevel;
  }

  public void writeSignature(final JsonGenerator jsonGenerator, final ORecord record) throws IOException {
    if (settings.includeType) {
      jsonGenerator.writeStringField(ODocumentHelper.ATTRIBUTE_TYPE, String.valueOf((char) ORecordInternal.getRecordType(record)));
    }
    if (settings.includeId && record.getIdentity() != null && record.getIdentity().isValid()) {
      jsonGenerator.writeStringField(ODocumentHelper.ATTRIBUTE_RID, record.getIdentity().toString());
    }
    if (settings.includeVer) {
      jsonGenerator.writeNumberField(ODocumentHelper.ATTRIBUTE_VERSION, record.getVersion());
    }
    if (settings.includeClazz && record instanceof ODocument && ((ODocument) record).getClassName() != null) {
      jsonGenerator.writeStringField(ODocumentHelper.ATTRIBUTE_CLASS, ((ODocument) record).getClassName());
    }
  }

  public boolean fetchEmbeddedDocuments() {
    return settings.alwaysFetchEmbeddedDocuments;
  }

  protected void manageTypes(final String iFieldName, final Object iFieldValue, OType fieldType) {
    if (settings.keepTypes) {
      if (iFieldValue instanceof Long)
        appendType(typesStack.peek(), iFieldName, 'l');
      else if (iFieldValue instanceof OIdentifiable)
        appendType(typesStack.peek(), iFieldName, 'x');
      else if (iFieldValue instanceof Float)
        appendType(typesStack.peek(), iFieldName, 'f');
      else if (iFieldValue instanceof Short)
        appendType(typesStack.peek(), iFieldName, 's');
      else if (iFieldValue instanceof Double)
        appendType(typesStack.peek(), iFieldName, 'd');
      else if (iFieldValue instanceof Date)
        appendType(typesStack.peek(), iFieldName, 't');
      else if (iFieldValue instanceof Byte || iFieldValue instanceof byte[])
        appendType(typesStack.peek(), iFieldName, 'b');
      else if (iFieldValue instanceof BigDecimal)
        appendType(typesStack.peek(), iFieldName, 'c');
      else if (iFieldValue instanceof ORecordLazySet)
        appendType(typesStack.peek(), iFieldName, 'n');
      else if (iFieldValue instanceof Set<?>)
        appendType(typesStack.peek(), iFieldName, 'e');
      else if (iFieldValue instanceof ORidBag)
        appendType(typesStack.peek(), iFieldName, 'g');
      else {
        OType t = fieldType;
        if (t == null)
          t = OType.getTypeByValue(iFieldValue);
        if (t == OType.LINKLIST)
          appendType(typesStack.peek(), iFieldName, 'z');
        else if (t == OType.LINKMAP)
          appendType(typesStack.peek(), iFieldName, 'm');
        else if (t == OType.CUSTOM)
          appendType(typesStack.peek(), iFieldName, 'u');
      }
    }
  }

  private void appendType(final StringBuilder iBuffer, final String iFieldName, final char iType) {
    if (iBuffer.length() > 0)
      iBuffer.append(',');
    iBuffer.append(iFieldName);
    iBuffer.append('=');
    iBuffer.append(iType);
  }
}
