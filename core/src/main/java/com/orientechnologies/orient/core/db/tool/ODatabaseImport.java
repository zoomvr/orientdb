/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.db.tool;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODocumentFieldWalker;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.tool.importer.OConverterData;
import com.orientechnologies.orient.core.db.tool.importer.OLinksRewriter;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.hashindex.local.OHashIndexFactory;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.metadata.security.OIdentity;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

/**
 * Import data from a file into a database.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODatabaseImport extends ODatabaseImpExpAbstract {
  public static final String EXPORT_IMPORT_MAP_NAME          = "___exportImportRIDMap";
  public static final int    IMPORT_RECORD_DUMP_LAP_EVERY_MS = 5000;

  private       Map<OPropertyImpl, String> linkedClasses   = new HashMap<OPropertyImpl, String>();
  private       Map<OClass, List<String>>  superClasses    = new HashMap<OClass, List<String>>();
  private final JsonParser                 jsonParser;
  private       ORecord                    record;
  private       boolean                    schemaImported  = false;
  private       int                        exporterVersion = -1;
  private       ORID                       schemaRecordId;
  private       ORID                       indexMgrRecordId;

  private boolean deleteRIDMapping = true;

  protected OIndex<OIdentifiable> exportImportHashTable;

  private boolean preserveClusterIDs = true;
  private boolean migrateLinks       = true;
  private boolean merge              = false;
  private boolean rebuildIndexes     = true;

  private Set<String>         indexesToRebuild    = new HashSet<String>();
  private Map<String, String> convertedClassNames = new HashMap<String, String>();

  public ODatabaseImport(final ODatabaseDocumentInternal database, final String iFileName, final OCommandOutputListener iListener)
      throws IOException {
    super(database, iFileName, iListener);

    if (iListener == null)
      listener = new OCommandOutputListener() {
        @Override
        public void onMessage(String iText) {
        }
      };

    InputStream inStream;
    final BufferedInputStream bf = new BufferedInputStream(new FileInputStream(fileName));
    bf.mark(1024);
    try {
      inStream = new GZIPInputStream(bf, 16384); // 16KB
    } catch (Exception ignore) {
      bf.reset();
      inStream = bf;
    }

    JsonFactory jfactory = new JsonFactory();
    jsonParser = jfactory.createParser(new InputStreamReader(inStream));
    database.declareIntent(new OIntentMassiveInsert());
  }

  public ODatabaseImport(final ODatabaseDocumentInternal database, final InputStream iStream,
      final OCommandOutputListener iListener) throws IOException {
    super(database, "streaming", iListener);
    JsonFactory jfactory = new JsonFactory();
    jsonParser = jfactory.createParser(new InputStreamReader(iStream));
    database.declareIntent(new OIntentMassiveInsert());
  }

  @Override
  public ODatabaseImport setOptions(String iOptions) {
    super.setOptions(iOptions);
    return this;
  }

  @Override
  public void run() {
    importDatabase();
  }

  public ODatabaseImport importDatabase() {
    boolean preValidation = database.isValidationEnabled();
    try {
      listener.onMessage("\nStarted import of database '" + database.getURL() + "' from " + fileName + "...");

      long time = System.currentTimeMillis();

      jsonParser.setCodec(new JsonMapper());

      database.setValidationEnabled(false);

      database.setStatus(STATUS.IMPORTING);

      if (!merge) {
        removeDefaultNonSecurityClasses();
        database.getMetadata().getIndexManager().reload();
      }

      for (OIndex<?> index : database.getMetadata().getIndexManager().getIndexes()) {
        if (index.isAutomatic())
          indexesToRebuild.add(index.getName());
      }

      final ObjectMapper objectMapper = new ObjectMapper();
      String tag;
      boolean clustersImported = false;
      JsonToken jsonToken = jsonParser.nextToken();
      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        jsonToken = jsonParser.currentToken();
        if (jsonToken != JsonToken.FIELD_NAME) {
          throwInvalidFormat();
        }

        tag = jsonParser.getCurrentName();

        if (tag.equals("info"))
          importInfo();
        else if (tag.equals("clusters")) {
          importClusters();
          clustersImported = true;
        } else if (tag.equals("schema"))
          importSchema(clustersImported, objectMapper);
        else if (tag.equals("records"))
          importRecords();
        else if (tag.equals("indexes"))
          importIndexes(objectMapper);
        else if (tag.equals("manualIndexes"))
          importManualIndexes();
        else if (tag.equals("brokenRids")) {
          processBrokenRids();
          continue;
        } else
          throw new ODatabaseImportException("Invalid format. Found unsupported tag '" + tag + "'");
      }

      if (rebuildIndexes)
        rebuildIndexes();

      // This is needed to insure functions loaded into an open
      // in memory database are available after the import.
      // see issue #5245
      database.getMetadata().reload();

      database.getStorage().synch();
      database.setStatus(STATUS.OPEN);

      if (deleteRIDMapping) {
        removeExportImportRIDsMap();
      }

      listener.onMessage("\n\nDatabase import completed in " + ((System.currentTimeMillis() - time)) + " ms");

    } catch (Exception e) {
      final StringWriter writer = new StringWriter();
      writer.append("Error on database import \n");
      final PrintWriter printWriter = new PrintWriter(writer);
      e.printStackTrace(printWriter);
      printWriter.flush();

      listener.onMessage(writer.toString());

      try {
        writer.close();
      } catch (IOException e1) {
        throw new ODatabaseExportException("Error on importing database '" + database.getName() + "' from file: " + fileName, e1);
      }

      throw new ODatabaseExportException("Error on importing database '" + database.getName() + "' from file: " + fileName, e);
    } finally {
      database.setValidationEnabled(preValidation);
      close();
    }

    return this;
  }

  private void processBrokenRids() throws IOException, ParseException {
    Set<ORID> brokenRids = new HashSet<ORID>();
    processBrokenRids(brokenRids);
  }

  //just read collection so import process can continue
  private void processBrokenRids(Set<ORID> brokenRids) throws IOException, ParseException {
    if (exporterVersion >= 12) {
      listener.onMessage("Reading of set of RIDs of records which were detected as broken during database export\n");

      jsonParser.nextToken();
      JsonToken jsonToken = jsonParser.nextToken();
      if (jsonToken != JsonToken.START_ARRAY) {
        throwInvalidFormat();
      }

      while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
        final ORecordId recordId = new ORecordId(jsonParser.getText());
        brokenRids.add(recordId);
      }
    }
    if (migrateLinks) {
      if (exporterVersion >= 12)
        listener.onMessage(
            brokenRids.size() + " were detected as broken during database export, links on those records will be removed from"
                + " result database");
      migrateLinksInImportedDocuments(brokenRids);
    }
  }

  public void rebuildIndexes() {
    database.getMetadata().getIndexManager().reload();

    OIndexManager indexManager = database.getMetadata().getIndexManager();

    listener.onMessage("\nRebuild of stale indexes...");
    for (String indexName : indexesToRebuild) {

      if (indexManager.getIndex(indexName) == null) {
        listener.onMessage("\nIndex " + indexName + " is skipped because it is absent in imported DB.");
        continue;
      }

      listener.onMessage("\nStart rebuild index " + indexName);
      database.command(new OCommandSQL("rebuild index " + indexName));
      listener.onMessage("\nRebuild  of index " + indexName + " is completed.");
    }
    listener.onMessage("\nStale indexes were rebuilt...");
  }

  public ODatabaseImport removeExportImportRIDsMap() {
    listener.onMessage("\nDeleting RID Mapping table...");
    if (exportImportHashTable != null) {
      database.command(new OCommandSQL("drop index " + EXPORT_IMPORT_MAP_NAME));
      exportImportHashTable = null;
    }

    listener.onMessage("OK\n");
    return this;
  }

  public void close() {
    try {
      jsonParser.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    database.declareIntent(null);
  }

  public boolean isMigrateLinks() {
    return migrateLinks;
  }

  public void setMigrateLinks(boolean migrateLinks) {
    this.migrateLinks = migrateLinks;
  }

  public boolean isRebuildIndexes() {
    return rebuildIndexes;
  }

  public void setRebuildIndexes(boolean rebuildIndexes) {
    this.rebuildIndexes = rebuildIndexes;
  }

  public boolean isPreserveClusterIDs() {
    return preserveClusterIDs;
  }

  public void setPreserveClusterIDs(boolean preserveClusterIDs) {
    this.preserveClusterIDs = preserveClusterIDs;
  }

  public boolean isMerge() {
    return merge;
  }

  public void setMerge(boolean merge) {
    this.merge = merge;
  }

  public boolean isDeleteRIDMapping() {
    return deleteRIDMapping;
  }

  public void setDeleteRIDMapping(boolean deleteRIDMapping) {
    this.deleteRIDMapping = deleteRIDMapping;
  }

  @Override
  protected void parseSetting(final String option, final List<String> items) {
    if (option.equalsIgnoreCase("-deleteRIDMapping"))
      deleteRIDMapping = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-preserveClusterIDs"))
      preserveClusterIDs = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-merge"))
      merge = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-migrateLinks"))
      migrateLinks = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-rebuildIndexes"))
      rebuildIndexes = Boolean.parseBoolean(items.get(0));
    else
      super.parseSetting(option, items);
  }

  public void setOption(final String option, String value) {
    parseSetting("-" + option, Arrays.asList(value));
  }

  protected void removeDefaultClusters() {
    listener.onMessage(
        "\nWARN: Exported database does not support manual index separation." + " Manual index cluster will be dropped.");

    // In v4 new cluster for manual indexes has been implemented. To keep database consistent we should shift back
    // all clusters and recreate cluster for manual indexes in the end.
    database.dropCluster(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, true);

    final OSchema schema = database.getMetadata().getSchema();
    if (schema.existsClass(OUser.CLASS_NAME))
      schema.dropClass(OUser.CLASS_NAME);
    if (schema.existsClass(ORole.CLASS_NAME))
      schema.dropClass(ORole.CLASS_NAME);
    if (schema.existsClass(OSecurityShared.RESTRICTED_CLASSNAME))
      schema.dropClass(OSecurityShared.RESTRICTED_CLASSNAME);
    if (schema.existsClass(OFunction.CLASS_NAME))
      schema.dropClass(OFunction.CLASS_NAME);
    if (schema.existsClass("ORIDs"))
      schema.dropClass("ORIDs");
    if (schema.existsClass(OClassTrigger.CLASSNAME))
      schema.dropClass(OClassTrigger.CLASSNAME);
    schema.save();

    database.dropCluster(OStorage.CLUSTER_DEFAULT_NAME, true);

    database.getStorage().setDefaultClusterId(database.addCluster(OStorage.CLUSTER_DEFAULT_NAME));

    // Starting from v4 schema has been moved to internal cluster.
    // Create a stub at #2:0 to prevent cluster position shifting.
    new ODocument().save(OStorage.CLUSTER_DEFAULT_NAME);

    database.getMetadata().getSecurity().create();
  }

  private void importInfo() throws IOException, ParseException {
    listener.onMessage("\nImporting database info...");

    JsonToken jsonToken = jsonParser.nextToken();
    if (jsonToken != JsonToken.START_OBJECT) {
      throwInvalidFormat();
      return;
    }

    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      final String fieldName = jsonParser.getCurrentName();
      if (fieldName.equals("exporter-version")) {
        jsonParser.nextToken();
        exporterVersion = jsonParser.getIntValue();
      } else if (fieldName.equals("schemaRecordId")) {
        jsonParser.nextToken();
        schemaRecordId = new ORecordId(jsonParser.getText());
      } else if (fieldName.equals("indexMgrRecordId")) {
        jsonParser.nextToken();
        indexMgrRecordId = new ORecordId(jsonParser.getText());
      } else {
        jsonParser.nextToken();
      }
    }

    if (schemaRecordId == null)
      schemaRecordId = new ORecordId(database.getStorage().getConfiguration().getSchemaRecordId());

    if (indexMgrRecordId == null)
      indexMgrRecordId = new ORecordId(database.getStorage().getConfiguration().getIndexMgrRecordId());

    listener.onMessage("OK");
  }

  private void throwInvalidFormat() {
    throw new ODatabaseException("Invalid format of exported data");
  }

  private void removeDefaultNonSecurityClasses() {
    listener.onMessage("\nNon merge mode (-merge=false): removing all default non security classes");

    OSchema schema = database.getMetadata().getSchema();
    Collection<OClass> classes = schema.getClasses();
    OClass orole = schema.getClass(ORole.CLASS_NAME);
    OClass ouser = schema.getClass(OUser.CLASS_NAME);
    OClass oidentity = schema.getClass(OIdentity.CLASS_NAME);
    final Map<String, OClass> classesToDrop = new HashMap<String, OClass>();
    final Set<String> indexes = new HashSet<String>();
    for (OClass dbClass : classes) {
      String className = dbClass.getName();

      if (!dbClass.isSuperClassOf(orole) && !dbClass.isSuperClassOf(ouser) && !dbClass.isSuperClassOf(oidentity)) {
        classesToDrop.put(className, dbClass);
        for (OIndex<?> index : dbClass.getIndexes()) {
          indexes.add(index.getName());
        }
      }
    }

    final OIndexManager indexManager = database.getMetadata().getIndexManager();
    for (String indexName : indexes) {
      indexManager.dropIndex(indexName);
    }

    int removedClasses = 0;
    while (!classesToDrop.isEmpty()) {
      final AbstractList<String> classesReadyToDrop = new ArrayList<String>();
      for (String className : classesToDrop.keySet()) {
        boolean isSuperClass = false;
        for (OClass dbClass : classesToDrop.values()) {
          List<OClass> parentClasses = dbClass.getSuperClasses();
          if (parentClasses != null) {
            for (OClass parentClass : parentClasses) {
              if (className.equalsIgnoreCase(parentClass.getName())) {
                isSuperClass = true;
                break;
              }
            }
          }
        }
        if (!isSuperClass) {
          classesReadyToDrop.add(className);
        }
      }
      for (String className : classesReadyToDrop) {
        schema.dropClass(className);
        classesToDrop.remove(className);
        removedClasses++;
        listener.onMessage("\n- Class " + className + " was removed.");
      }
    }

    schema.save();
    schema.reload();

    listener.onMessage("\nRemoved " + removedClasses + " classes.");
  }

  private void importManualIndexes() throws IOException, ParseException {
    listener.onMessage("\nImporting manual index entries...");

    ODocument doc = new ODocument();

    OIndexManager indexManager = database.getMetadata().getIndexManager();
    // FORCE RELOADING
    int n = 0;
    JsonToken jsonToken = jsonParser.nextToken();
    if (jsonToken != JsonToken.START_ARRAY) {
      throwInvalidFormat();
    }

    long tot = 0;
    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      OIndex<?> index = null;
      tot++;

      String indexName = null;
      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        final String fieldName = jsonParser.getCurrentName();
        if (fieldName.equals("name")) {
          jsonParser.nextToken();
          indexName = jsonParser.getText();
          index = database.getMetadata().getIndexManager().getIndex(indexName);

        } else if (fieldName.equals("content")) {
          jsonToken = jsonParser.nextToken();
          if (jsonToken != JsonToken.START_ARRAY) {
            throwInvalidFormat();
          }

          if (index == null || indexName.equals(EXPORT_IMPORT_MAP_NAME)) {
            jsonParser.skipChildren();
            continue;
          }

          while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            final TreeNode entryNode = jsonParser.readValueAsTree();

            doc = (ODocument) ORecordSerializerJSON.INSTANCE.fromString(entryNode.toString(), doc, null);
            doc.setLazyLoad(false);

            final OIdentifiable oldRid = doc.field("rid");
            final OIdentifiable newRid;
            if (!doc.<Boolean>field("binary")) {
              if (exportImportHashTable != null)
                newRid = exportImportHashTable.get(oldRid);
              else
                newRid = oldRid;

              index.put(doc.field("key"), newRid != null ? newRid.getIdentity() : oldRid.getIdentity());
            } else {
              ORuntimeKeyIndexDefinition<?> runtimeKeyIndexDefinition = (ORuntimeKeyIndexDefinition<?>) index.getDefinition();
              OBinarySerializer<?> binarySerializer = runtimeKeyIndexDefinition.getSerializer();

              if (exportImportHashTable != null)
                newRid = exportImportHashTable.get(doc.<OIdentifiable>field("rid")).getIdentity();
              else
                newRid = doc.field("rid");

              index.put(binarySerializer.deserialize((byte[]) doc.field("key"), 0), newRid != null ? newRid : oldRid);
            }
          }
        }
      }

      if (index != null) {
        listener.onMessage("OK (" + tot + " entries)");
        n++;
      } else
        listener.onMessage("ERR, the index wasn't found in configuration");
    }

    listener.onMessage("\nDone. Imported " + String.format("%,d", n) + " indexes.");
  }

  private void importSchema(boolean clustersImported, final ObjectMapper objectMapper) throws IOException, ParseException {
    if (!clustersImported) {
      removeDefaultClusters();
    }

    listener.onMessage("\nImporting database schema...");

    long classImported = 0;

    JsonToken jsonToken = jsonParser.nextToken();
    if (jsonToken != JsonToken.START_OBJECT) {
      throwInvalidFormat();
    }
    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      final String fieldName = jsonParser.getCurrentName();

      if (fieldName.equals("blob-clusters")) {
        jsonToken = jsonParser.nextToken();
        if (jsonToken != JsonToken.START_ARRAY) {
          throwInvalidFormat();
        }
        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
          final int clusterId = jsonParser.getIntValue();
          if (!database.getBlobClusterIds().contains(clusterId)) {
            String name = database.getClusterNameById(clusterId);
            database.addBlobCluster(name);
          }
        }
      } else if (fieldName.equals("classes")) {
        jsonToken = jsonParser.nextToken();
        if (jsonToken != JsonToken.START_ARRAY) {
          throwInvalidFormat();
        }
        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
          if (jsonParser.currentToken() != JsonToken.START_OBJECT) {
            throwInvalidFormat();
          }

          try {
            final TreeNode classNode = jsonParser.readValueAsTree();
            String className = ((ValueNode) classNode.get("name")).asText();

            if (className.contains(".")) {
              // MIGRATE OLD NAME WITH . TO _
              final String newClassName = className.replace('.', '_');
              convertedClassNames.put(className, newClassName);

              listener.onMessage("\nWARNING: class '" + className + "' has been renamed in '" + newClassName + "'\n");

              className = newClassName;
            }

            final int classDefClusterId;

            if (classNode.get("default-cluster-id") != null) {
              classDefClusterId = ((ValueNode) classNode.get("default-cluster-id")).asInt();
            } else {
              classDefClusterId = database.getDefaultClusterId();
            }

            OClassImpl cls = (OClassImpl) database.getMetadata().getSchema().getClass(className);

            if (cls != null) {
              if (cls.getDefaultClusterId() != classDefClusterId)
                cls.setDefaultClusterId(classDefClusterId);
            } else if (clustersImported) {
              cls = (OClassImpl) database.getMetadata().getSchema().createClass(className, new int[] { classDefClusterId });
            } else if (className.equalsIgnoreCase("ORestricted")) {
              cls = (OClassImpl) database.getMetadata().getSchema().createAbstractClass(className);
            } else {
              cls = (OClassImpl) database.getMetadata().getSchema().createClass(className);
            }

            int[] classClusterIds = objectMapper.treeToValue(classNode.get("cluster-ids"), int[].class);

            if (clustersImported) {
              for (int clusterId : classClusterIds) {
                if (clusterId >= 0) {
                  cls.addClusterId(clusterId);
                }
              }
            }

            if (classNode.get("strictMode") != null) {
              cls.setStrictMode(((ValueNode) classNode.get("strictMode")).asBoolean());
            }

            if (classNode.get("abstract") != null) {
              cls.setAbstract(((ValueNode) classNode.get("abstract")).asBoolean());
            }

            if (classNode.get("oversize") != null) {
              cls.setOverSize(Float.parseFloat(((ValueNode) classNode.get("oversize")).asText()));
            }

            if (classNode.get("short-name") != null) {
              final String shortName = ((ValueNode) classNode.get("short-name")).asText();

              if (!cls.getName().equalsIgnoreCase(shortName)) {
                cls.setShortName(shortName);
              }
            }

            if (classNode.get("super-class") != null) {
              final List<String> superClassNames = new ArrayList<String>();
              superClassNames.add(((ValueNode) classNode.get("super-class")).asText());

              superClasses.put(cls, superClassNames);
            }

            if (classNode.get("super-classes") != null) {
              final String[] classes = objectMapper.treeToValue(classNode.get("super-classes"), String[].class);
              superClasses.put(cls, Arrays.asList(classes));
            }

            if (classNode.get("customFields") != null) {
              @SuppressWarnings("unchecked")
              final Map<String, String> customFields = objectMapper.treeToValue(classNode.get("customFields"), Map.class);
              for (Entry<String, String> entry : customFields.entrySet()) {
                cls.setCustom(entry.getKey(), entry.getValue());
              }
            }

            if (classNode.get("cluster-selection") != null) {
              cls.setClusterSelection(((ValueNode) classNode.get("cluster-selection")).asText());
            }

            if (classNode.get("properties") != null) {
              importProperties(cls, classNode.get("properties"), objectMapper);
            }
          } catch (final Exception ex) {
            OLogManager.instance().error(this, "Error on importing schema", ex);
            listener.onMessage("ERROR (" + classImported + " entries): " + ex);
          }

          classImported++;
        }

      }
    }

    // REBUILD ALL THE INHERITANCE
    for (Map.Entry<OClass, List<String>> entry : superClasses.entrySet())
      for (String s : entry.getValue()) {
        OClass superClass = database.getMetadata().getSchema().getClass(s);

        if (!entry.getKey().getSuperClasses().contains(superClass))
          entry.getKey().addSuperClass(superClass);
      }

    // SET ALL THE LINKED CLASSES
    for (Map.Entry<OPropertyImpl, String> entry : linkedClasses.entrySet()) {
      entry.getKey().setLinkedClass(database.getMetadata().getSchema().getClass(entry.getValue()));
    }

    database.getMetadata().getSchema().save();

    if (exporterVersion < 11) {
      OClass role = database.getMetadata().getSchema().getClass("ORole");
      role.dropProperty("rules");
    }

    listener.onMessage("OK (" + classImported + " classes)");
    schemaImported = true;
  }

  private void importProperties(final OClass iClass, final TreeNode propertiesNode, final ObjectMapper objectMapper)
      throws IOException {

    for (int i = 0; i < propertiesNode.size(); i++) {
      final TreeNode propertyNode = propertiesNode.get(i);

      importProperty(iClass, objectMapper, propertyNode);
    }
  }

  private void importProperty(OClass iClass, ObjectMapper objectMapper, TreeNode propertyNode)
      throws com.fasterxml.jackson.core.JsonProcessingException {
    final String propName = ((ValueNode) propertyNode.get("name")).asText();
    final OType type = OType.valueOf(((ValueNode) propertyNode.get("type")).asText());

    OPropertyImpl prop = (OPropertyImpl) iClass.getProperty(propName);
    if (prop == null) {
      // CREATE IT
      prop = (OPropertyImpl) iClass.createProperty(propName, type, (OType) null, true);
    }

    if (propertyNode.get("customFields") != null) {
      @SuppressWarnings("unchecked")
      Map<String, String> customFields = objectMapper.treeToValue(propertyNode.get("customFields"), Map.class);
      for (Entry<String, String> entry : customFields.entrySet()) {
        prop.setCustom(entry.getKey(), entry.getValue());
      }
    }

    if (propertyNode.get("min") != null) {
      prop.setMin(((ValueNode) propertyNode.get("min")).asText());
    }

    if (propertyNode.get("max") != null) {
      prop.setMax(((ValueNode) propertyNode.get("max")).asText());
    }

    if (propertyNode.get("linked-class") != null) {
      linkedClasses.put(prop, ((ValueNode) propertyNode.get("linked-class")).asText());
    }

    if (propertyNode.get("mandatory") != null) {
      prop.setMandatory(Boolean.parseBoolean(((ValueNode) propertyNode.get("mandatory")).asText()));
    }

    if (propertyNode.get("readonly") != null) {
      prop.setReadonly(Boolean.parseBoolean(((ValueNode) propertyNode.get("readonly")).asText()));
    }

    if (propertyNode.get("not-null") != null) {
      prop.setNotNull(Boolean.parseBoolean(((ValueNode) propertyNode.get("not-null")).asText()));
    }

    if (propertyNode.get("linked-type") != null) {
      prop.setLinkedType(OType.valueOf(((ValueNode) propertyNode.get("linked-type")).asText()));
    }

    if (propertyNode.get("collate") != null) {
      prop.setCollate(((ValueNode) propertyNode.get("collate")).asText());
    }

    if (propertyNode.get("default-value") != null) {
      prop.setDefaultValue(((ValueNode) propertyNode.get("default-value")).asText());
    }

    if (propertyNode.get("customFields") != null) {
      @SuppressWarnings("unchecked")
      Map<String, String> customFields = objectMapper.treeToValue(propertyNode.get("customFields"), Map.class);

      for (Entry<String, String> entry : customFields.entrySet()) {
        prop.setCustom(entry.getKey(), entry.getValue());
      }
    }

    if (propertyNode.get("regexp") != null) {
      prop.setRegexp(((ValueNode) propertyNode.get("regexp")).asText());
    }
  }

  private long importClusters() throws ParseException, IOException {
    listener.onMessage("\nImporting clusters...");

    long total = 0;

    JsonToken jsonToken = jsonParser.nextToken();
    if (jsonToken != JsonToken.START_ARRAY) {
      throwInvalidFormat();
    }

    boolean recreateManualIndex = false;
    if (exporterVersion <= 4) {
      removeDefaultClusters();
      recreateManualIndex = true;
    }

    final Set<String> indexesToRebuild = new HashSet<String>();

    @SuppressWarnings("unused")
    ORecordId rid = null;
    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      jsonToken = jsonParser.nextToken();
      if (jsonToken != JsonToken.FIELD_NAME) {
        throwInvalidFormat();
      }

      if (!jsonParser.getText().equals("name")) {
        throwInvalidFormat();
      }

      jsonParser.nextToken();
      String name = jsonParser.getText();

      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        //just skip whole the object
        if (name == null) {
          continue;
        }

        if (includeClusters != null) {
          if (!includeClusters.contains(name)) {
            continue;
          }
        } else if (excludeClusters != null) {
          if (excludeClusters.contains(name)) {
            continue;
          }
        }

        String fieldName = jsonParser.getCurrentName();
        if (fieldName.equals("id")) {
          jsonParser.nextToken();

          int id = jsonParser.getIntValue();

          listener.onMessage("\n- Creating cluster '" + name + "' ...");
          int clusterId = database.getClusterIdByName(name);
          if (clusterId == -1) {
            // CREATE IT
            if (!preserveClusterIDs)
              clusterId = database.addCluster(name);
            else {
              clusterId = database.addCluster(name, id, (Object[]) null);
              assert clusterId == id;
            }
          }

          if (clusterId != id) {
            if (!preserveClusterIDs) {
              if (database.countClusterElements(clusterId - 1) == 0) {
                listener.onMessage("Found previous version: migrating old clusters...");
                database.dropCluster(name, true);
                database.addCluster("temp_" + clusterId, (Object[]) null);
                clusterId = database.addCluster(name);
              } else
                throw new OConfigurationException(
                    "Imported cluster '" + name + "' has id=" + clusterId + " different from the original: " + id
                        + ". To continue the import drop the cluster '" + database.getClusterNameById(clusterId - 1) + "' that has "
                        + database.countClusterElements(clusterId - 1) + " records");
            } else {
              database.dropCluster(clusterId, false);
              database.addCluster(name, id, (Object[]) null);
            }
          }

          if (!(name.equalsIgnoreCase(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME) || name
              .equalsIgnoreCase(OMetadataDefault.CLUSTER_INTERNAL_NAME) || name
              .equalsIgnoreCase(OMetadataDefault.CLUSTER_INDEX_NAME))) {
            if (!merge)
              database.command(new OCommandSQL("truncate cluster `" + name + "`")).execute();

            for (OIndex existingIndex : database.getMetadata().getIndexManager().getIndexes()) {
              if (existingIndex.getClusters().contains(name)) {
                indexesToRebuild.add(existingIndex.getName());
              }
            }
          }

          listener.onMessage("OK, assigned id=" + clusterId);

          total++;

        } else {
          jsonParser.nextToken();
          jsonParser.readValueAsTree();
        }
      }
    }

    listener.onMessage("\nRebuilding indexes of truncated clusters ...");

    for (final String indexName : indexesToRebuild)
      database.getMetadata().getIndexManager().getIndex(indexName).rebuild(new OProgressListener() {
        private long last = 0;

        @Override
        public void onBegin(Object iTask, long iTotal, Object metadata) {
          listener.onMessage("\n- Cluster content was updated: rebuilding index '" + indexName + "'...");
        }

        @Override
        public boolean onProgress(Object iTask, long iCounter, float iPercent) {
          final long now = System.currentTimeMillis();
          if (last == 0)
            last = now;
          else if (now - last > 1000) {
            listener.onMessage(String.format("\nIndex '%s' is rebuilding (%.2f/100)", indexName, iPercent));
            last = now;
          }
          return true;
        }

        @Override
        public void onCompletition(Object iTask, boolean iSucceed) {
          listener.onMessage(" Index " + indexName + " was successfully rebuilt.");
        }
      });

    listener.onMessage("\nDone " + indexesToRebuild.size() + " indexes were rebuilt.");

    if (recreateManualIndex) {
      database.addCluster(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
      database.getMetadata().getIndexManager().create();

      listener.onMessage("\nManual index cluster was recreated.");
    }

    listener.onMessage("\nDone. Imported " + total + " clusters");

    if (database.load(new ORecordId(database.getStorage().getConfiguration().getIndexMgrRecordId())) == null) {
      ODocument indexDocument = new ODocument();
      indexDocument.save(OMetadataDefault.CLUSTER_INTERNAL_NAME);

      database.getStorage().setIndexMgrRecordId(indexDocument.getIdentity().toString());
    }

    return total;
  }

  private long importRecords() throws Exception {
    long total = 0;

    database.getMetadata().getIndexManager().dropIndex(EXPORT_IMPORT_MAP_NAME);
    OIndexFactory factory = OIndexes
        .getFactory(OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString(), OHashIndexFactory.HASH_INDEX_ALGORITHM);

    exportImportHashTable = (OIndex<OIdentifiable>) database.getMetadata().getIndexManager()
        .createIndex(EXPORT_IMPORT_MAP_NAME, OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString(),
            new OSimpleKeyIndexDefinition(0, OType.LINK), null, null, null);

    JsonToken jsonToken = jsonParser.nextToken();
    if (jsonToken != JsonToken.START_ARRAY) {
      throwInvalidFormat();
    }

    long totalRecords = 0;

    listener.onMessage("\n\nImporting records...");

    ORID rid;
    ORID lastRid = new ORecordId();
    final long begin = System.currentTimeMillis();
    long lastLapRecords = 0;
    long last = begin;
    Set<String> involvedClusters = new HashSet<String>();

    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      jsonToken = jsonParser.currentToken();
      if (jsonToken != JsonToken.START_OBJECT) {
        throwInvalidFormat();
      }

      final TreeNode recordNode = jsonParser.readValueAsTree();

      rid = importRecord(recordNode);

      total++;
      if (rid != null) {
        ++lastLapRecords;
        ++totalRecords;

        if (rid.getClusterId() != lastRid.getClusterId() || involvedClusters.isEmpty())
          involvedClusters.add(database.getClusterNameById(rid.getClusterId()));
        lastRid = rid;
      }

      final long now = System.currentTimeMillis();
      if (now - last > IMPORT_RECORD_DUMP_LAP_EVERY_MS) {
        final List<String> sortedClusters = new ArrayList<String>(involvedClusters);
        Collections.sort(sortedClusters);

        listener.onMessage(String.format("\n- Imported %,d records into clusters: %s. "
                + "Total JSON records imported so for %,d .Total records imported so far: %,d (%,.2f/sec)", lastLapRecords, total,
            sortedClusters.size(), totalRecords, (float) lastLapRecords * 1000 / (float) IMPORT_RECORD_DUMP_LAP_EVERY_MS));

        // RESET LAP COUNTERS
        last = now;
        lastLapRecords = 0;
        involvedClusters.clear();
      }

      record = null;
    }

    final Set<ORID> brokenRids = new HashSet<ORID>();
    processBrokenRids(brokenRids);

    listener.onMessage(String.format("\n\nDone. Imported %,d records in %,.2f secs\n", totalRecords,
        ((float) (System.currentTimeMillis() - begin)) / 1000));

    return total;
  }

  private ORID importRecord(final TreeNode treeNode) throws Exception {
    String value = treeNode.toString();

    // JUMP EMPTY RECORDS
    while (!value.isEmpty() && value.charAt(0) != '{') {
      value = value.substring(1);
    }

    record = null;
    try {

      try {
        record = ORecordSerializerJSON.INSTANCE.fromString(value, record, null);
      } catch (OSerializationException e) {
        if (e.getCause() instanceof OSchemaException) {
          // EXTRACT CLASS NAME If ANY
          final int pos = value.indexOf("\"@class\":\"");
          if (pos > -1) {
            final int end = value.indexOf("\"", pos + "\"@class\":\"".length() + 1);
            final String value1 = value.substring(0, pos + "\"@class\":\"".length());
            final String clsName = value.substring(pos + "\"@class\":\"".length(), end);
            final String value2 = value.substring(end);

            final String newClassName = convertedClassNames.get(clsName);

            value = value1 + newClassName + value2;
            // OVERWRITE CLASS NAME WITH NEW NAME
            record = ORecordSerializerJSON.INSTANCE.fromString(value, record, null);
          }
        } else
          throw OException.wrapException(new ODatabaseImportException("Error on importing record"), e);
      }

      // Incorrect record format , skip this record
      if (record == null || record.getIdentity() == null) {
        OLogManager.instance().warn(this, "Broken record was detected and will be skipped");
        return null;
      }

      if (schemaImported && record.getIdentity().equals(schemaRecordId)) {
        // JUMP THE SCHEMA
        return null;
      }

      // CHECK IF THE CLUSTER IS INCLUDED
      if (includeClusters != null) {
        if (!includeClusters.contains(database.getClusterNameById(record.getIdentity().getClusterId()))) {
          return null;
        }
      } else if (excludeClusters != null) {
        if (excludeClusters.contains(database.getClusterNameById(record.getIdentity().getClusterId())))
          return null;
      }

      if (record instanceof ODocument && excludeClasses != null) {
        if (excludeClasses.contains(((ODocument) record).getClassName())) {
          return null;
        }
      }

      if (record.getIdentity().getClusterId() == 0 && record.getIdentity().getClusterPosition() == 1)
        // JUMP INTERNAL RECORDS
        return null;

      if (exporterVersion >= 3) {
        int oridsId = database.getClusterIdByName("ORIDs");
        int indexId = database.getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME);

        if (record.getIdentity().getClusterId() == indexId || record.getIdentity().getClusterId() == oridsId)
          // JUMP INDEX RECORDS
          return null;
      }

      final int manualIndexCluster = database.getClusterIdByName(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
      final int internalCluster = database.getClusterIdByName(OMetadataDefault.CLUSTER_INTERNAL_NAME);
      final int indexCluster = database.getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME);

      if (exporterVersion >= 4) {
        if (record.getIdentity().getClusterId() == manualIndexCluster)
          // JUMP INDEX RECORDS
          return null;
      }

      if (record.getIdentity().equals(indexMgrRecordId))
        return null;

      final ORID rid = record.getIdentity();

      final int clusterId = rid.getClusterId();

      if ((clusterId != manualIndexCluster && clusterId != internalCluster && clusterId != indexCluster)) {
        ORecordInternal.setVersion(record, 0);
        record.setDirty();
        ORecordInternal.setIdentity(record, new ORecordId());

        if (!preserveRids && record instanceof ODocument && ODocumentInternal.getImmutableSchemaClass(((ODocument) record)) != null)
          record.save();
        else
          record.save(database.getClusterNameById(clusterId));

        if (!rid.equals(record.getIdentity()))
          // SAVE IT ONLY IF DIFFERENT
          exportImportHashTable.put(rid, record.getIdentity());
      }

    } catch (Exception t) {
      if (record != null)
        OLogManager.instance().error(this,
            "Error importing record " + record.getIdentity() + ". Source line " + jsonParser.getCurrentLocation().getLineNr()
                + ", column " + jsonParser.getCurrentLocation().getColumnNr(), t);
      else
        OLogManager.instance().error(this,
            "Error importing record. Source line " + jsonParser.getCurrentLocation().getLineNr() + ", column " + jsonParser
                .getCurrentLocation().getColumnNr(), t);

      if (!(t instanceof ODatabaseException)) {
        throw t;
      }

    }

    return record.getIdentity();
  }

  private void importIndexes(final ObjectMapper objectMapper) throws IOException, ParseException {
    listener.onMessage("\n\nImporting indexes ...");

    OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();
    indexManager.reload();

    JsonToken jsonToken = jsonParser.nextToken();
    if (jsonToken != JsonToken.START_ARRAY) {
      throwInvalidFormat();
    }

    int n = 0;
    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      jsonToken = jsonParser.currentToken();
      if (jsonToken != JsonToken.START_OBJECT) {
        throwInvalidFormat();
      }

      TreeNode indexNode = jsonParser.readValueAsTree();

      String blueprintsIndexClass = null;
      String indexName = null;
      String indexType = null;
      String indexAlgorithm = null;
      Set<String> clustersToIndex = new HashSet<String>();
      OIndexDefinition indexDefinition = null;
      ODocument metadata = null;
      Map<String, String> engineProperties = null;

      if (indexNode.get("name") != null) {
        indexName = ((ValueNode) indexNode.get("name")).asText();
      }
      if (indexNode.get("type") != null) {
        indexType = ((ValueNode) indexNode.get("type")).asText();
      }
      if (indexNode.get("algorithm") != null) {
        indexAlgorithm = ((ValueNode) indexNode.get("algorithm")).asText();
      }

      if (indexNode.get("clustersToIndex") != null) {
        //noinspection unchecked
        clustersToIndex = objectMapper.treeToValue(indexNode.get("clustersToIndex"), Set.class);
      }

      if (indexNode.get("definition") != null) {
        TreeNode definitionNode = indexNode.get("definition");
        final String className = ((ValueNode) definitionNode.get("defClass")).asText();
        final TreeNode definitionStream = definitionNode.get("stream");

        final ODocument indexDefinitionDoc = (ODocument) ORecordSerializerJSON.INSTANCE
            .fromString(definitionStream.toString(), null, null);
        try {
          final Class<?> indexDefClass = Class.forName(className);
          indexDefinition = (OIndexDefinition) indexDefClass.getDeclaredConstructor().newInstance();
          indexDefinition.fromStream(indexDefinitionDoc);
        } catch (final ClassNotFoundException e) {
          throw new IOException("Error during deserialization of index definition", e);
        } catch (final NoSuchMethodException e) {
          throw new IOException("Error during deserialization of index definition", e);
        } catch (final InvocationTargetException e) {
          throw new IOException("Error during deserialization of index definition", e);
        } catch (final InstantiationException e) {
          throw new IOException("Error during deserialization of index definition", e);
        } catch (final IllegalAccessException e) {
          throw new IOException("Error during deserialization of index definition", e);
        }
      }

      if (indexNode.get("metadata") != null) {
        final String jsonMetadata = indexNode.get("metadata").toString();
        metadata = new ODocument().fromJSON(jsonMetadata);
      }

      if (indexNode.get("engineProperties") != null) {
        Map<String, Object> map = new ODocument().fromJSON(indexNode.get("engineProperties").toString()).toMap();
        if (map != null) {
          engineProperties = new HashMap<String, String>(map.size());
          for (Entry<String, Object> entry : map.entrySet()) {
            engineProperties.put(entry.getKey(), entry.getValue().toString());
          }
        }
      }

      if (indexNode.get("blueprintsIndexClass") != null) {
        blueprintsIndexClass = ((ValueNode) indexNode.get("blueprintsIndexClass")).asText();
      }

      if (indexName == null)
        throw new IllegalArgumentException("Index name is missing");

      // drop automatically created indexes
      if (!indexName.equalsIgnoreCase(EXPORT_IMPORT_MAP_NAME)) {
        listener.onMessage("\n- Index '" + indexName + "'...");

        indexManager.dropIndex(indexName);
        indexesToRebuild.remove(indexName);
        List<Integer> clusterIds = new ArrayList<Integer>();

        for (final String clusterName : clustersToIndex) {
          int id = database.getClusterIdByName(clusterName);
          if (id != -1)
            clusterIds.add(id);
          else
            listener.onMessage(
                String.format("found not existent cluster '%s' in index '%s' configuration, skipping", clusterName, indexName));
        }
        int[] clusterIdsToIndex = new int[clusterIds.size()];

        int i = 0;
        for (Integer clusterId : clusterIds) {
          clusterIdsToIndex[i] = clusterId;
          i++;
        }

        boolean oldValue = false;
        if (indexDefinition != null) {
          oldValue = OGlobalConfiguration.INDEX_IGNORE_NULL_VALUES_DEFAULT.getValueAsBoolean();
          OGlobalConfiguration.INDEX_IGNORE_NULL_VALUES_DEFAULT.setValue(indexDefinition.isNullValuesIgnored());
        }
        final OIndex index = indexManager
            .createIndex(indexName, indexType, indexDefinition, clusterIdsToIndex, null, metadata, indexAlgorithm);
        if (indexDefinition != null) {
          OGlobalConfiguration.INDEX_IGNORE_NULL_VALUES_DEFAULT.setValue(oldValue);
        }
        if (blueprintsIndexClass != null) {
          ODocument configuration = index.getConfiguration();
          configuration.field("blueprintsIndexClass", blueprintsIndexClass);
          indexManager.save();
        }

        n++;
        listener.onMessage("OK");

      }
    }

    listener.onMessage("\nDone. Created " + n + " indexes.");
  }

  private void migrateLinksInImportedDocuments(Set<ORID> brokenRids) throws IOException {
    listener.onMessage("\n\nStarted migration of links (-migrateLinks=true). Links are going to be updated according to new RIDs:");

    final long begin = System.currentTimeMillis();
    long last = begin;
    long documentsLastLap = 0;

    long totalDocuments = 0;
    Collection<String> clusterNames = database.getClusterNames();
    for (String clusterName : clusterNames) {
      if (OMetadataDefault.CLUSTER_INDEX_NAME.equals(clusterName) || OMetadataDefault.CLUSTER_INTERNAL_NAME.equals(clusterName)
          || OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME.equals(clusterName))
        continue;

      long documents = 0;
      String prefix = "";

      listener.onMessage("\n- Cluster " + clusterName + "...");

      final int clusterId = database.getClusterIdByName(clusterName);
      final long clusterRecords = database.countClusterElements(clusterId);
      OStorage storage = database.getStorage();

      OPhysicalPosition[] positions = storage.ceilingPhysicalPositions(clusterId, new OPhysicalPosition(0));
      while (positions.length > 0) {
        for (OPhysicalPosition position : positions) {
          ORecord record = database.load(new ORecordId(clusterId, position.clusterPosition));
          if (record instanceof ODocument) {
            ODocument document = (ODocument) record;
            rewriteLinksInDocument(document, brokenRids);

            documents++;
            documentsLastLap++;
            totalDocuments++;

            final long now = System.currentTimeMillis();
            if (now - last > IMPORT_RECORD_DUMP_LAP_EVERY_MS) {
              listener.onMessage(String.format("\n--- Migrated %,d of %,d records (%,.2f/sec)", documents, clusterRecords,
                  (float) documentsLastLap * 1000 / (float) IMPORT_RECORD_DUMP_LAP_EVERY_MS));

              // RESET LAP COUNTERS
              last = now;
              documentsLastLap = 0;
              prefix = "\n---";
            }
          }
        }

        positions = storage.higherPhysicalPositions(clusterId, positions[positions.length - 1]);
      }

      listener.onMessage(String.format("%s Completed migration of %,d records in current cluster", prefix, documents));
    }

    listener.onMessage(String.format("\nTotal links updated: %,d", totalDocuments));
  }

  protected void rewriteLinksInDocument(ODocument document, Set<ORID> brokenRids) {
    rewriteLinksInDocument(document, exportImportHashTable, brokenRids);

    document.save();
  }

  protected static void rewriteLinksInDocument(ODocument document, OIndex<OIdentifiable> exportImportHashTable,
      Set<ORID> brokenRids) {
    final OLinksRewriter rewriter = new OLinksRewriter(new OConverterData(exportImportHashTable, brokenRids));
    final ODocumentFieldWalker documentFieldWalker = new ODocumentFieldWalker();
    documentFieldWalker.walkDocument(document, rewriter);
  }

}