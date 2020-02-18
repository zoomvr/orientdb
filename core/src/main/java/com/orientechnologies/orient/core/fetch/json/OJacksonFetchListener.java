/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
 *
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
 * limitations under the License.
 */
package com.orientechnologies.orient.core.fetch.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.util.ODateHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * @author luca.molino
 */
public class OJacksonFetchListener implements OFetchListener {
  private final String format;

  public OJacksonFetchListener(String format) {
    this.format = format;
  }

  public boolean requireFieldProcessing() {
    return true;
  }

  public void processStandardField(final ODocument record, final Object fieldValue, final String fieldName,
      final OFetchContext context, final Object usObject, final String format, OType filedType) {
    try {
      writeAttribute(((OJacksonFetchContext) context).getJsonGenerator(), fieldName, fieldValue, format, filedType);
    } catch (IOException e) {
      throw OException
          .wrapException(new OFetchException("Error processing field '" + fieldValue + " of record " + record.getIdentity()), e);
    }
  }

  private static void writeAttribute(final JsonGenerator jsonGenerator, final String name, final Object value, final String format,
      OType valueType) throws IOException {
    if (name != null) {
      jsonGenerator.writeFieldName(name);
    }

    if (format != null && format.contains("graph") && name != null && (name.startsWith("in_") || name.startsWith("out_")) && (
        value == null || value instanceof OIdentifiable)) {
      // FORCE THE OUTPUT AS COLLECTION
      jsonGenerator.writeStartArray();
      if (value instanceof OIdentifiable) {
        final boolean shallow = format != null && format.contains("shallow");
        if (shallow)
          jsonGenerator.writeNumber(1);
        else
          writeValue(jsonGenerator, value, format, null);
      }
      jsonGenerator.writeEndArray();
    } else
      writeValue(jsonGenerator, value, format, valueType);
  }

  private static void writeValue(final JsonGenerator jsonGenerator, final Object value, final String format, OType valueType)
      throws IOException {
    if (value == null) {
      jsonGenerator.writeNull();
    }
    final boolean oldAutoConvertSettings;

    if (value instanceof ORecordLazyMultiValue) {
      oldAutoConvertSettings = ((ORecordLazyMultiValue) value).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) value).setAutoConvertToRecord(false);
    } else
      oldAutoConvertSettings = false;

    if (value instanceof Boolean) {
      jsonGenerator.writeBoolean((Boolean) value);
    } else if (value instanceof Integer) {
      jsonGenerator.writeNumber((Integer) value);
    } else if (value instanceof Long) {
      jsonGenerator.writeNumber((Long) value);
    } else if (value instanceof Short) {
      jsonGenerator.writeNumber((Short) value);
    } else if (value instanceof Float) {
      jsonGenerator.writeNumber((Float) value);
    } else if (value instanceof Double) {
      jsonGenerator.writeNumber((Double) value);
    } else if (value instanceof BigInteger) {
      jsonGenerator.writeNumber((BigInteger) value);
    } else if (value instanceof BigDecimal) {
      jsonGenerator.writeNumber((BigDecimal) value);
    } else if (value instanceof OIdentifiable) {
      final OIdentifiable linked = (OIdentifiable) value;
      if (linked.getIdentity().isValid()) {
        jsonGenerator.writeString(linked.getIdentity().toString());
      } else {
        if (format != null && format.contains("shallow")) {
          jsonGenerator.writeStartObject();
          jsonGenerator.writeEndObject();
        } else {
          final ORecord rec = linked.getRecord();
          if (rec != null) {
            ORecordSerializerJSON.toJSON((ODocument) rec, jsonGenerator, format);
          } else {
            jsonGenerator.writeNull();
          }
        }
      }

    } else if (value.getClass().isArray()) {
      if (value instanceof byte[]) {
        final byte[] source = (byte[]) value;

        if (format != null && format.contains("shallow"))
          jsonGenerator.writeNumber(source.length);
        else
          jsonGenerator.writeBinary(source);
      } else {
        jsonGenerator.writeStartArray();
        int size = Array.getLength(value);
        if (format != null && format.contains("shallow"))
          jsonGenerator.writeNumber(size);
        else {
          jsonGenerator.writeStartArray();
          for (int i = 0; i < size; ++i) {
            writeValue(jsonGenerator, Array.get(value, i), format, null);
          }
          jsonGenerator.writeEndArray();
        }
      }
    } else if (value instanceof Iterator<?>)
      iteratorToJSON((Iterator<?>) value, format, jsonGenerator);
    else if (value instanceof Iterable<?>)
      iteratorToJSON(((Iterable<?>) value).iterator(), format, jsonGenerator);

    else if (value instanceof Map<?, ?>)
      mapToJSON((Map<Object, Object>) value, format, jsonGenerator);

    else if (value instanceof Map.Entry<?, ?>) {
      final Map.Entry<?, ?> entry = (Map.Entry<?, ?>) value;
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName(entry.getKey().toString());
      writeValue(jsonGenerator, entry.getValue(), format, null);
      jsonGenerator.writeEndObject();
    } else if (value instanceof Date) {
      if (format.indexOf("dateAsLong") > -1)
        jsonGenerator.writeNumber(((Date) value).getTime());
      else {
        jsonGenerator.writeString(ODateHelper.getDateTimeFormatInstance().format(value));
      }
    } else if (value instanceof ORecordLazyMultiValue)
      iteratorToJSON(((ORecordLazyMultiValue) value).rawIterator(), format, jsonGenerator);
    else if (value instanceof Iterable<?>)
      iteratorToJSON(((Iterable<?>) value).iterator(), format, jsonGenerator);

    else {
      if (valueType == null)
        valueType = OType.getTypeByValue(value);

      if (valueType == OType.CUSTOM) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream object = new ObjectOutputStream(baos);
        object.writeObject(value);
        object.flush();
        jsonGenerator.writeBinary(baos.toByteArray());
      } else {
        // TREAT IT AS STRING
        final String v = value.toString();
        jsonGenerator.writeString(v);
      }
    }

    if (value instanceof ORecordLazyMultiValue)
      ((ORecordLazyMultiValue) value).setAutoConvertToRecord(oldAutoConvertSettings);
  }

  private static void iteratorToJSON(final Iterator<?> it, final String format, final JsonGenerator jsonGenerator)
      throws IOException {
    jsonGenerator.writeStartArray();
    if (format != null && format.contains("shallow")) {
      if (it instanceof OMultiCollectionIterator<?>)
        jsonGenerator.writeNumber(((OMultiCollectionIterator<?>) it).size());
      else {
        // COUNT THE MULTI VALUE
        int i;
        for (i = 0; it.hasNext(); ++i)
          it.next();
        jsonGenerator.writeNumber(i);
      }
    } else {
      for (int i = 0; it.hasNext(); ++i) {
        writeValue(jsonGenerator, it.next(), format, null);
      }
    }
    jsonGenerator.writeEndArray();
  }

  private static void mapToJSON(final Map<?, ?> map, final String format, final JsonGenerator jsonGenerator) {
    try {
      jsonGenerator.writeStartObject();
      if (map != null) {
        int i = 0;
        Map.Entry<?, ?> entry;
        for (Iterator<?> it = map.entrySet().iterator(); it.hasNext(); ++i) {
          entry = (Map.Entry<?, ?>) it.next();
          jsonGenerator.writeFieldName(entry.getKey().toString());
          writeValue(jsonGenerator, entry.getValue(), format, null);
        }
      }
      jsonGenerator.writeEndObject();
    } catch (IOException e) {
      throw OException.wrapException(new OSerializationException("Error on serializing map"), e);
    }
  }

  public void processStandardCollectionValue(final Object iFieldValue, final OFetchContext iContext) throws OFetchException {
    try {
      writeValue(((OJacksonFetchContext) iContext).getJsonGenerator(), iFieldValue, "", null);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on processStandardCollectionValue", e);
    }
  }

  public Object fetchLinked(final ODocument iRecord, final Object iUserObject, final String iFieldName, final ODocument iLinked,
      final OFetchContext iContext) throws OFetchException {
    return iLinked;
  }

  public Object fetchLinkedMapEntry(final ODocument iRecord, final Object iUserObject, final String iFieldName, final String iKey,
      final ODocument iLinked, final OFetchContext iContext) throws OFetchException {
    return iLinked;
  }

  public void parseLinked(final ODocument iRootRecord, final OIdentifiable iLinked, final Object iUserObject,
      final String iFieldName, final OFetchContext iContext) throws OFetchException {
    try {
      writeLinkedAttribute(((OJacksonFetchContext)iContext).getJsonGenerator(), iLinked, iFieldName);
    } catch (IOException e) {
      throw OException.wrapException(new OFetchException(
          "Error writing linked field " + iFieldName + " (record:" + iLinked.getIdentity() + ") of record " + iRootRecord
              .getIdentity()), e);
    }
  }

  private void writeLinkedAttribute(final JsonGenerator jsonGenerator, final OIdentifiable iRecord, final String iFieldName)
      throws IOException {
    final String link = iRecord.getIdentity().isValid() ? iRecord.getIdentity().toString() : null;
    writeAttribute(jsonGenerator, iFieldName, link, format, null);
  }

  public void parseLinkedCollectionValue(ODocument iRootRecord, OIdentifiable iLinked, Object iUserObject, String iFieldName,
      OFetchContext iContext) throws OFetchException {
    try {
      if (((OJacksonFetchContext) iContext).isInCollection(iRootRecord)) {
        ((OJacksonFetchContext) iContext).writeLinkedValue(iLinked, iFieldName);
      } else {
        ((OJacksonFetchContext) iContext).writeLinkedAttribute(iLinked, iFieldName);
      }
    } catch (IOException e) {
      throw OException.wrapException(new OFetchException(
          "Error writing linked field " + iFieldName + " (record:" + iLinked.getIdentity() + ") of record " + iRootRecord
              .getIdentity()), e);
    }
  }

  public Object fetchLinkedCollectionValue(ODocument iRoot, Object iUserObject, String iFieldName, ODocument iLinked,
      OFetchContext iContext) throws OFetchException {
    return iLinked;
  }

  @Override
  public void skipStandardField(ODocument iRecord, String iFieldName, OFetchContext iContext, Object iUserObject, String iFormat)
      throws OFetchException {
  }
}
