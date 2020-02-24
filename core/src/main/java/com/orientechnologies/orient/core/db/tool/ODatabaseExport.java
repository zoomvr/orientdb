/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.db.tool;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

/**
 * Export data from a database to a file.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODatabaseExport extends ODatabaseImpExpAbstract {
  public static final int VERSION = 12;

  protected JsonGenerator writer;
  protected long          recordExported;
  protected int           compressionLevel  = Deflater.BEST_SPEED;
  protected int           compressionBuffer = 16384;              // 16Kb

  public ODatabaseExport(final ODatabaseDocumentInternal iDatabase, final String iFileName, final OCommandOutputListener iListener)
      throws IOException {
    super(iDatabase, iFileName, iListener);

    if (fileName == null)
      throw new IllegalArgumentException("file name missing");

    if (!fileName.endsWith(".gz")) {
      fileName += ".gz";
    }
    final File f = new File(fileName);
    if (f.getParentFile() != null)
      f.getParentFile().mkdirs();
    if (f.exists())
      f.delete();

    final GZIPOutputStream gzipOS = new GZIPOutputStream(new FileOutputStream(fileName), compressionBuffer) {
      {
        def.setLevel(compressionLevel);
      }
    };

    JsonFactory jfactory = new JsonFactory();
    writer = jfactory.createGenerator(gzipOS, JsonEncoding.UTF8);
    writer.setCodec(new JsonMapper());

    writer.writeStartObject();
  }

  public ODatabaseExport(final ODatabaseDocumentInternal iDatabase, final OutputStream outputStream,
      final OCommandOutputListener iListener) throws IOException {
    super(iDatabase, "streaming", iListener);

    JsonFactory jfactory = new JsonFactory();
    writer = jfactory.createGenerator(outputStream, JsonEncoding.UTF8);
    writer.setCodec(new JsonMapper());

    writer.writeStartObject();
  }

  @Override
  public void run() {
    exportDatabase();
  }

  @Override
  public ODatabaseExport setOptions(final String s) {
    super.setOptions(s);
    return this;
  }

  public ODatabaseExport exportDatabase() {
    try {
      listener.onMessage("\nStarted export of database '" + database.getName() + "' to " + fileName + "...");

      long time = System.currentTimeMillis();

      final ObjectMapper objectMapper = new ObjectMapper();
      if (includeInfo)
        exportInfo();
      if (includeClusterDefinitions)
        exportClusters();
      if (includeSchema)
        exportSchema(objectMapper);
      if (includeRecords)
        exportRecords();
      if (includeIndexDefinitions)
        exportIndexDefinitions(objectMapper);
      if (includeManualIndexes)
        exportManualIndexes();

      listener.onMessage("\n\nDatabase export completed in " + (System.currentTimeMillis() - time) + "ms");

      writer.flush();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on exporting database '%s' to: %s", e, database.getName(), fileName);
      throw new ODatabaseExportException("Error on exporting database '" + database.getName() + "' to: " + fileName, e);
    } finally {
      close();
    }
    return this;
  }

  public long exportRecords() throws IOException {
    long totalFoundRecords = 0;
    long totalExportedRecords = 0;

    int level = 1;
    listener.onMessage("\nExporting records...");

    final Set<ORID> brokenRids = new HashSet<ORID>();

    writer.writeArrayFieldStart("records");
    int exportedClusters = 0;
    int maxClusterId = getMaxClusterId();
    for (int i = 0; exportedClusters <= maxClusterId; ++i) {
      String clusterName = database.getClusterNameById(i);

      exportedClusters++;

      long clusterExportedRecordsTot = 0;

      if (clusterName != null) {
        // CHECK IF THE CLUSTER IS INCLUDED
        if (includeClusters != null) {
          if (!includeClusters.contains(clusterName.toUpperCase(Locale.ENGLISH)))
            continue;
        } else if (excludeClusters != null) {
          if (excludeClusters.contains(clusterName.toUpperCase(Locale.ENGLISH)))
            continue;
        }

        if (excludeClusters != null && excludeClusters.contains(clusterName.toUpperCase(Locale.ENGLISH)))
          continue;

        clusterExportedRecordsTot = database.countClusterElements(clusterName);
      } else if (includeClusters != null && !includeClusters.isEmpty())
        continue;

      listener.onMessage("\n- Cluster " + (clusterName != null ? "'" + clusterName + "'" : "NULL") + " (id=" + i + ")...");

      long clusterExportedRecordsCurrent = 0;

      if (clusterName != null) {
        ORecord rec = null;
        try {
          ORecordIteratorCluster<ORecord> it = database.browseCluster(clusterName);

          for (; it.hasNext(); ) {

            rec = it.next();
            if (rec instanceof ODocument) {
              // CHECK IF THE CLASS OF THE DOCUMENT IS INCLUDED
              ODocument doc = (ODocument) rec;
              final String className = doc.getClassName() != null ? doc.getClassName().toUpperCase(Locale.ENGLISH) : null;
              if (includeClasses != null) {
                if (!includeClasses.contains(className))
                  continue;
              } else if (excludeClasses != null) {
                if (excludeClasses.contains(className))
                  continue;
              }
            } else if (includeClasses != null && !includeClasses.isEmpty())
              continue;

            if (exportRecord(clusterExportedRecordsTot, clusterExportedRecordsCurrent, rec, brokenRids))
              clusterExportedRecordsCurrent++;
          }

          brokenRids.addAll(it.getBrokenRIDs());
        } catch (IOException e) {
          OLogManager.instance().error(this, "\nError on exporting record %s because of I/O problems", e, rec.getIdentity());
          // RE-THROW THE EXCEPTION UP
          throw e;
        } catch (OIOException e) {
          OLogManager.instance()
              .error(this, "\nError on exporting record %s because of I/O problems", e, rec == null ? null : rec.getIdentity());
          // RE-THROW THE EXCEPTION UP
          throw e;
        } catch (Exception e) {
          if (rec != null) {
            final byte[] buffer = rec.toStream();

            OLogManager.instance().error(this,
                "\nError on exporting record %s. It seems corrupted; size: %d bytes, raw content (as string):\n==========\n%s\n==========",
                e, rec.getIdentity(), buffer.length, new String(buffer));
          }
        }
      }

      listener.onMessage("OK (records=" + clusterExportedRecordsCurrent + "/" + clusterExportedRecordsTot + ")");

      totalExportedRecords += clusterExportedRecordsCurrent;
      totalFoundRecords += clusterExportedRecordsTot;
    }
    writer.writeEndArray();

    listener.onMessage(
        "\n\nDone. Exported " + totalExportedRecords + " of total " + totalFoundRecords + " records. " + brokenRids.size()
            + " records were detected as broken\n");

    writer.writeArrayFieldStart("brokenRids");

    boolean firsBrokenRid = true;

    for (ORID rid : brokenRids) {
      writer.writeString(rid.toString());

    }

    writer.writeEndArray();

    return totalExportedRecords;
  }

  public void close() {
    database.declareIntent(null);

    if (writer == null)
      return;

    try {
      writer.writeEndObject();
      writer.close();
      writer = null;
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error during closing of write stream", e);
    }
  }

  protected int getMaxClusterId() {
    int totalCluster = -1;
    for (String clusterName : database.getClusterNames()) {
      if (database.getClusterIdByName(clusterName) > totalCluster)
        totalCluster = database.getClusterIdByName(clusterName);
    }
    return totalCluster;
  }

  @Override
  protected void parseSetting(final String option, final List<String> items) {
    if (option.equalsIgnoreCase("-compressionLevel"))
      compressionLevel = Integer.parseInt(items.get(0));
    else if (option.equalsIgnoreCase("-compressionBuffer"))
      compressionBuffer = Integer.parseInt(items.get(0));
    else
      super.parseSetting(option, items);
  }

  private void exportClusters() throws IOException {
    listener.onMessage("\nExporting clusters...");

    writer.writeArrayFieldStart("clusters");
    int exportedClusters = 0;

    int maxClusterId = getMaxClusterId();

    for (int clusterId = 0; clusterId <= maxClusterId; ++clusterId) {

      final String clusterName = database.getClusterNameById(clusterId);

      // exclude removed clusters
      if (clusterName == null)
        continue;

      // CHECK IF THE CLUSTER IS INCLUDED
      if (includeClusters != null) {
        if (!includeClusters.contains(clusterName.toUpperCase(Locale.ENGLISH)))
          continue;
      } else if (excludeClusters != null) {
        if (excludeClusters.contains(clusterName.toUpperCase(Locale.ENGLISH)))
          continue;
      }

      writer.writeStartObject();

      writer.writeStringField("name", clusterName);
      writer.writeNumberField("id", clusterId);

      exportedClusters++;
      writer.writeEndObject();
    }

    listener.onMessage("OK (" + exportedClusters + " clusters)");

    writer.writeEndArray();
  }

  private void exportInfo() throws IOException {
    listener.onMessage("\nExporting database info...");

    writer.writeObjectFieldStart("info");
    writer.writeStringField("name", database.getName().replace('\\', '/'));
    writer.writeNumberField("default-cluster-id", database.getDefaultClusterId());
    writer.writeNumberField("exporter-version", VERSION);
    writer.writeStringField("engine-version", OConstants.getVersion());
    final String engineBuild = OConstants.getBuildNumber();
    if (engineBuild != null)
      writer.writeStringField("engine-build", engineBuild);
    writer.writeNumberField("storage-config-version", OStorageConfiguration.CURRENT_VERSION);
    writer.writeNumberField("schema-version", OSchemaShared.CURRENT_VERSION_NUMBER);
    writer.writeStringField("schemaRecordId", database.getStorage().getConfiguration().getSchemaRecordId());
    writer.writeStringField("indexMgrRecordId", database.getStorage().getConfiguration().getIndexMgrRecordId());
    writer.writeEndObject();

    listener.onMessage("OK");
  }

  private void exportIndexDefinitions(ObjectMapper objectMapper) throws IOException {
    listener.onMessage("\nExporting index info...");
    writer.writeArrayFieldStart("indexes");

    final OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();
    indexManager.reload();

    final Collection<? extends OIndex<?>> indexes = indexManager.getIndexes();

    for (OIndex<?> index : indexes) {
      if (index.getName().equals(ODatabaseImport.EXPORT_IMPORT_MAP_NAME))
        continue;

      final String clsName = index.getDefinition() != null ? index.getDefinition().getClassName() : null;

      // CHECK TO FILTER CLASS
      if (includeClasses != null) {
        if (!includeClasses.contains(clsName))
          continue;
      } else if (excludeClasses != null) {
        if (excludeClasses.contains(clsName))
          continue;
      }

      listener.onMessage("\n- Index " + index.getName() + "...");
      writer.writeStartObject();
      writer.writeStringField("name", index.getName());
      writer.writeStringField("type", index.getType());
      if (index.getAlgorithm() != null)
        writer.writeStringField("algorithm", index.getAlgorithm());

      if (!index.getClusters().isEmpty()) {
        writer.writeFieldName("clustersToIndex");
        objectMapper.writeValue(writer, index.getClusters());
      }

      if (index.getDefinition() != null) {
        writer.writeObjectFieldStart("definition");

        writer.writeStringField("defClass", index.getDefinition().getClass().getName());
        writer.writeFieldName("stream");
        ORecordSerializerJSON.toJSON(index.getDefinition().toStream(), writer,
            "rid,version,class,type,attribSameRow,keepTypes,alwaysFetchEmbedded,fetchPlan:*:0");

        writer.writeEndObject();
      }

      final ODocument metadata = index.getMetadata();
      if (metadata != null) {
        writer.writeFieldName("metadata");
        ORecordSerializerJSON
            .toJSON(metadata, writer, "rid,version,class,type,attribSameRow,keepTypes,alwaysFetchEmbedded,fetchPlan:*:0");
      }

      final ODocument configuration = index.getConfiguration();
      if (configuration.field("blueprintsIndexClass") != null)
        writer.writeStringField("blueprintsIndexClass", configuration.field("blueprintsIndexClass").toString());

      writer.writeEndObject();
      listener.onMessage("OK");
    }

    writer.writeEndArray();
    listener.onMessage("\nOK (" + indexes.size() + " indexes)");
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void exportManualIndexes() throws IOException {
    listener.onMessage("\nExporting manual indexes content...");

    final OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();
    indexManager.reload();

    final Collection<? extends OIndex<?>> indexes = indexManager.getIndexes();

    ODocument exportEntry = new ODocument();

    int manualIndexes = 0;
    writer.writeArrayFieldStart("manualIndexes");
    for (OIndex<?> index : indexes) {
      if (index.getName().equals(ODatabaseImport.EXPORT_IMPORT_MAP_NAME))
        continue;

      if (!index.isAutomatic()) {
        listener.onMessage("\n- Exporting index " + index.getName() + " ...");

        writer.writeStartObject();
        writer.writeStringField("name", index.getName());

        List<ODocument> indexContent = database.query(new OSQLSynchQuery<ODocument>("select from index:" + index.getName()));

        writer.writeArrayFieldStart("content");

        int i = 0;
        for (ODocument indexEntry : indexContent) {
          indexEntry.setLazyLoad(false);
          final OIndexDefinition indexDefinition = index.getDefinition();

          exportEntry.reset();
          exportEntry.setLazyLoad(false);

          if (indexDefinition instanceof ORuntimeKeyIndexDefinition
              && ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer() != null) {
            final OBinarySerializer binarySerializer = ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer();

            final int dataSize = binarySerializer.getObjectSize(indexEntry.field("key"));
            final byte[] binaryContent = new byte[dataSize];
            binarySerializer.serialize(indexEntry.field("key"), binaryContent, 0);

            exportEntry.field("binary", true);
            exportEntry.field("key", binaryContent);
          } else {
            exportEntry.field("binary", false);
            exportEntry.field("key", indexEntry.field("key"));
          }

          exportEntry.field("rid", indexEntry.field("rid"));

          i++;

          ORecordSerializerJSON
              .toJSON(exportEntry, writer, "rid,version,class,type,attribSameRow,keepTypes,alwaysFetchEmbedded,fetchPlan:*:0");

          final long percent = indexContent.size() / 10;
          if (percent > 0 && (i % percent) == 0)
            listener.onMessage(".");
        }
        writer.writeEndArray();
        writer.writeEndObject();

        listener.onMessage("OK (entries=" + index.getSize() + ")");
        manualIndexes++;
      }
    }
    writer.writeEndArray();
    listener.onMessage("\nOK (" + manualIndexes + " manual indexes)");
  }

  private void exportSchema(ObjectMapper objectMapper) throws IOException {
    listener.onMessage("\nExporting schema...");

    writer.writeObjectFieldStart("schema");
    OSchema s = database.getMetadata().getImmutableSchemaSnapshot();
    writer.writeNumberField("version", s.getVersion());
    writer.writeFieldName("blob-clusters");
    objectMapper.writeValue(writer, database.getBlobClusterIds());
    if (!s.getClasses().isEmpty()) {
      writer.writeArrayFieldStart("classes");

      final List<OClass> classes = new ArrayList<OClass>(s.getClasses());
      Collections.sort(classes);

      for (OClass cls : classes) {
        // CHECK TO FILTER CLASS
        if (includeClasses != null) {
          if (!includeClasses.contains(cls.getName().toUpperCase(Locale.ENGLISH)))
            continue;
        } else if (excludeClasses != null) {
          if (excludeClasses.contains(cls.getName().toUpperCase(Locale.ENGLISH)))
            continue;
        }

        writer.writeStartObject();
        writer.writeStringField("name", cls.getName());
        writer.writeNumberField("default-cluster-id", cls.getDefaultClusterId());
        writer.writeFieldName("cluster-ids");
        objectMapper.writeValue(writer, cls.getClusterIds());

        if (cls.getOverSize() > 1)
          writer.writeNumberField("oversize", cls.getClassOverSize());
        if (cls.isStrictMode())
          writer.writeBooleanField("strictMode", cls.isStrictMode());
        if (!cls.getSuperClasses().isEmpty()) {
          writer.writeFieldName("super-classes");
          objectMapper.writeValue(writer, cls.getSuperClassesNames());
        }
        if (cls.getShortName() != null)
          writer.writeStringField("short-name", cls.getShortName());
        if (cls.isAbstract())
          writer.writeBooleanField("abstract", cls.isAbstract());
        writer.writeStringField("cluster-selection", cls.getClusterSelection().getName()); // @SINCE 1.7

        if (!cls.properties().isEmpty()) {
          writer.writeArrayFieldStart("properties");

          final List<OProperty> properties = new ArrayList<OProperty>(cls.declaredProperties());
          Collections.sort(properties);

          for (OProperty p : properties) {
            writer.writeStartObject();
            writer.writeStringField("name", p.getName());
            writer.writeStringField("type", p.getType().toString());
            if (p.isMandatory())
              writer.writeBooleanField("mandatory", p.isMandatory());
            if (p.isReadonly())
              writer.writeBooleanField("readonly", p.isReadonly());
            if (p.isNotNull())
              writer.writeBooleanField("not-null", p.isNotNull());
            if (p.getLinkedClass() != null)
              writer.writeStringField("linked-class", p.getLinkedClass().getName());
            if (p.getLinkedType() != null)
              writer.writeStringField("linked-type", p.getLinkedType().toString());
            if (p.getMin() != null)
              writer.writeStringField("min", p.getMin());
            if (p.getMax() != null)
              writer.writeStringField("max", p.getMax());
            if (p.getCollate() != null)
              writer.writeStringField("collate", p.getCollate().getName());
            if (p.getDefaultValue() != null)
              writer.writeStringField("default-value", p.getDefaultValue());
            if (p.getRegexp() != null)
              writer.writeStringField("regexp", p.getRegexp());
            final Set<String> customKeys = p.getCustomKeys();
            final Map<String, String> custom = new HashMap<String, String>();
            for (String key : customKeys)
              custom.put(key, p.getCustom(key));

            if (!custom.isEmpty()) {
              writer.writeFieldName("customFields");
              objectMapper.writeValue(writer, custom);
            }

            writer.writeEndObject();
          }
          writer.writeEndArray();
        }
        final Set<String> customKeys = cls.getCustomKeys();
        final Map<String, String> custom = new HashMap<String, String>();
        for (String key : customKeys)
          custom.put(key, cls.getCustom(key));

        if (!custom.isEmpty()) {
          writer.writeFieldName("customFields");
          objectMapper.writeValue(writer, custom);
        }

        writer.writeEndObject();
      }
      writer.writeEndArray();
    }

    writer.writeEndObject();

    listener.onMessage("OK (" + s.getClasses().size() + " classes)");
  }

  private boolean exportRecord(long recordTot, long recordNum, ORecord rec, Set<ORID> brokenRids) throws IOException {
    if (rec != null)
      try {
        if (rec.getIdentity().isValid())
          rec.reload();

        if (rec.getIdentity().equals(new ORecordId("#29:0"))) {
          System.out.println();
        }
        ORecordSerializerJSON.toJSON(rec, writer, "rid,type,version,class,attribSameRow,keepTypes,alwaysFetchEmbedded,dateAsLong");
        recordExported++;
        recordNum++;

        if (recordTot > 10 && (recordNum + 1) % (recordTot / 10) == 0)
          listener.onMessage(".");

        return true;
      } catch (Exception e) {
        if (rec != null) {
          final ORID rid = rec.getIdentity().copy();

          if (rid != null) {
            brokenRids.add(rid);
          }

          final byte[] buffer = rec.toStream();

          OLogManager.instance().error(this,
              "\nError on exporting record %s. It seems corrupted; size: %d bytes, raw content (as string):\n==========\n%s\n==========",
              e, rec.getIdentity(), buffer.length, new String(buffer));
        }
      }

    return false;
  }
}
