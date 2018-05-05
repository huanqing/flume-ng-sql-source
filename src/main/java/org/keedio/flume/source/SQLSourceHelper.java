package org.keedio.flume.source;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.cfg.Configuration;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import static org.json.simple.parser.ParseException.*;

import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.criteria.CriteriaBuilder;

/**
 * Helper to manage configuration parameters and utility methods <p>
 * <p>
 * Configuration parameters readed from flume configuration file:
 * <tt>type: </tt> org.keedio.flume.source.SQLSource <p>
 * <tt>table: </tt> table to read from <p>
 * <tt>columns.to.select: </tt> columns to select for import data (* will import all) <p>
 * <tt>run.query.delay: </tt> delay time to execute each query to database <p>
 * <tt>status.file.path: </tt> Directory to save status file <p>
 * <tt>status.file.name: </tt> Name for status file (saves last row index processed) <p>
 * <tt>batch.size: </tt> Batch size to send events from flume source to flume channel <p>
 * <tt>max.rows: </tt> Max rows to import from DB in one query <p>
 * <tt>custom.query: </tt> Custom query to execute to database (be careful) <p>
 *
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 * @author <a href="mailto:lalazaro@keedio.com">Luis Lazaro</a>
 */

public class SQLSourceHelper {

  private static final Logger LOG = LoggerFactory.getLogger(SQLSourceHelper.class);

  private File file, directory;
  private int runQueryDelay, batchSize, maxRows;
  private String statusFilePath, statusFileName, connectionURL, tables,
    columnsToSelect, customQuery, query, sourceName, delimiterEntry, connectionUserName, connectionPassword,
		defaultCharsetResultSet;
  private Boolean encloseByQuotes;

  private Context context;

  private Map<String, String> statusFileJsonMap = new LinkedHashMap<String, String>();
  protected List<Table> tableList = new LinkedList<Table>();

  private boolean readOnlySession;

  private static final String DEFAULT_STATUS_DIRECTORY = "/var/lib/flume";
  private static final int DEFAULT_QUERY_DELAY = 10000;
  private static final int DEFAULT_BATCH_SIZE = 100;
  private static final int DEFAULT_MAX_ROWS = 10000;
  private static final String DEFAULT_DELIMITER_ENTRY = ",";
  private static final Boolean DEFAULT_ENCLOSE_BY_QUOTES = true;


  private static final String DEFAULT_CHARSET_RESULTSET = "UTF-8";

  //private  SimpleDateFormat SDF = new SimpleDateFormat("yyyyMMddHHmmss");
  public static   String dateFormat = "yyyy-MM-dd HH:mm:ss";
  private  String jsonLineEnd = "\n";

  /**
   * Builds an SQLSourceHelper containing the configuration parameters and
   * usefull utils for SQL Source
   *
   * @param context    Flume source context, contains the properties from configuration file
   * @param sourceName source file name for store status
   */
  public SQLSourceHelper(Context context, String sourceName) {

    this.context = context;

    statusFilePath = context.getString("status.file.path", DEFAULT_STATUS_DIRECTORY);
    statusFileName = context.getString("status.file.name");
    tables = context.getString("tables");
    columnsToSelect = context.getString("columns.to.select", "*");
    runQueryDelay = context.getInteger("run.query.delay", DEFAULT_QUERY_DELAY);
    directory = new File(statusFilePath);
    customQuery = context.getString("custom.query");
    batchSize = context.getInteger("batch.size", DEFAULT_BATCH_SIZE);
    maxRows = context.getInteger("max.rows", DEFAULT_MAX_ROWS);
    connectionURL = context.getString("hibernate.connection.url");
    connectionUserName = context.getString("hibernate.connection.user");
    connectionPassword = context.getString("hibernate.connection.password");
    readOnlySession = context.getBoolean("read.only", false);
    jsonLineEnd = context.getString("json.line.end", "\n");
    dateFormat  = context.getString("date.format", dateFormat);
    LOG.info("tables:" + tables);
    LOG.info(context.toString());
    for(String table : tables.split(",")) {
      Table t = new Table();
      t.setName(table);
      LOG.info("table:" + table);
      t.setiField(context.getString(table + ".ifield"));
      t.setiFieldType(context.getString( table + ".ifieldtype"));
      t.setiFieldIndex(context.getString(table + ".ifieldindex"));

      t.setStartFrom(context.getString( table + ".startfrom"));
      t.setStaticFields(context.getString(table + ".staticfields"));
      t.setStaticValues(context.getString(table + ".staticvalues"));
      t.setFields(context.getString(table + ".fields"));
      t.setFieldsShow(context.getString(table + ".fieldsshow"));

      tableList.add(t);
    }
    //加载配置文件 读取startFrom信息

    this.sourceName = sourceName;
    delimiterEntry = context.getString("delimiter.entry", DEFAULT_DELIMITER_ENTRY);
    encloseByQuotes = context.getBoolean("enclose.by.quotes", DEFAULT_ENCLOSE_BY_QUOTES);
    statusFileJsonMap = new LinkedHashMap();
    defaultCharsetResultSet = context.getString("default.charset.resultset", DEFAULT_CHARSET_RESULTSET);

    checkMandatoryProperties();

    if (!(isStatusDirectoryCreated())) {
      createDirectory();
    }

    file = new File(statusFilePath + "/" + statusFileName);

    if (!isStatusFileCreated()) {
      createStatusFile();
    } else {
      getStatusFileIndex(tableList);
    }

  }



  private boolean isStatusFileCreated() {
    return file.exists() && !file.isDirectory() ? true : false;
  }

  private boolean isStatusDirectoryCreated() {
    return directory.exists() && !directory.isFile() ? true : false;
  }

  /**
   * Converter from a List of Object List to a List of String arrays <p>
   * Useful for csvWriter
   *
   * @param queryResult Query Result from hibernate executeQuery method
   * @return A list of String arrays, ready for csvWriter.writeall method
   */
  public List<Map<String,Object>> getAllRows(List<Map<String,Object>> queryResult, Table table) {

  /*  List<String[]> allRows = new ArrayList<String[]>();

    if (queryResult == null || queryResult.isEmpty()) {
      return allRows;
    }

    String[] row = null;
    int iFieldIndex = table.getiFieldIndex();
    for (int i = 0; i < queryResult.size(); i++) {
      List<Object> rawRow = queryResult.get(i);
      row = new String[rawRow.size()];

      for (int j = 0; j < rawRow.size(); j++) {
        if (rawRow.get(j) != null) {
          row[j] = rawRow.get(j).toString();
          if(j == iFieldIndex) {
            table.changeStartFrom(row[j]);
          }
        } else {
          row[j] = "";
        }
      }

      allRows.add(addStatic(row, table.getStaticValues()));
    }*/

  /*  List<Map<String,Object>> allRows = new LinkedList<>();

    if (queryResult == null || queryResult.isEmpty()) {
      return allRows;
    }
    for(int i = 0; i < queryResult.size(); i++) {
      Map<String, Object> rawRow = queryResult.get(i);
      Object iField = rawRow.get(table.getiField());
      if(iField != null) {

      }
    }*/

    return queryResult;
  }
  public String[] addStatic(String[] row, List<String> add) {
    int addSize = add.size();
    if(addSize > 0) {
      String[] added = new String[row.length + addSize];
      int i = 0;
      for(String r : row) {
        added[i] = r;
        i++;
      }
      for(String a : add) {
        added[i] = a;
        i++;
      }
      return added;
    }
    return  row;
  }


  /**
   * write the query result to the output stream
   * @param queryResult Query Result from hibernate executeQuery method
   * @param printWriter output stream object
   */
  public void writeAllRows(List<Map<String,Object>> queryResult,PrintWriter printWriter, Table table){

    if (queryResult == null || queryResult.isEmpty()) {
      return ;
    }
    SimpleDateFormat SDF = new SimpleDateFormat(dateFormat);
    Gson gson = new Gson();
    Type type = new TypeToken<Map<String,Object>>(){}.getType();
    for (Map<String,Object> item :queryResult) {
      for (Map.Entry<String,Object> entry:item.entrySet()) {
        Object value = entry.getValue();
        // initialize null field
        if (value == null){
          entry.setValue("");
        }
        // format time
        LOG.debug("valueType:" + value.getClass().toString());
        if (value instanceof  java.sql.Timestamp || value instanceof java.sql.Date || value instanceof java.util.Date){
          entry.setValue(SDF.format(value));
        }
      }
      Object iFieldRaw = item.get(table.getiField());
      if(iFieldRaw != null) {
        String iField = iFieldRaw.toString();
        if(StringUtils.isNotEmpty(iField)) {
          table.changeStartFrom(iField);
        }
      }


      Map<String, String> staticFieldsMap = table.getStaticFieldsMap();
      if(staticFieldsMap != null && staticFieldsMap.size() > 0) {
        item.putAll(staticFieldsMap);
      }
      String json = gson.toJson(item, type);
      // write to output stream
      LOG.debug("row json:" + json);
      printWriter.print(json+jsonLineEnd);
    }
  }

  /**
   * Create status file
   */
  public void createStatusFile() {

    for(Table table : tableList) {
      statusFileJsonMap.put(table.getName(), String.valueOf(table.getStartFrom()));
    }

    try {
      Writer fileWriter = new FileWriter(file, false);
      JSONValue.writeJSONString(statusFileJsonMap, fileWriter);
      fileWriter.close();
    } catch (IOException e) {
      LOG.error("Error creating value to status file!!!", e);
    }
  }

  /**
   * Update status file with last read row index
   */
  public void updateStatusFile() {
    for(Table table: tableList) {
      LOG.debug("updateStatusFile:" + table);
      statusFileJsonMap.put(table.getName(), String.valueOf(table.getStartFrom()));
    }
    try {
      Writer fileWriter = new FileWriter(file, false);
      JSONValue.writeJSONString(statusFileJsonMap, fileWriter);
      fileWriter.close();
    } catch (IOException e) {
      LOG.error("Error writing incremental value to status file!!!", e);
    }
  }

  public String buildQuery(Table table) {
    return String.format("select %s from %s where %s > ? order by %s", table.getFields(), table.getName(), table.getiField(), table.getiField());
  }

  private void getStatusFileIndex(List<Table> tableList) {

    if (!isStatusFileCreated()) {
      LOG.info("Status file not created, using start value from config file and creating file");
      return ;
    } else {
      try {
        FileReader fileReader = new FileReader(file);

        JSONParser jsonParser = new JSONParser();
        statusFileJsonMap = (Map) jsonParser.parse(fileReader);
        for(Table table : tableList) {
          String startFrom = statusFileJsonMap.get(table.getName());
          LOG.debug("reader from status file startFrom:" + startFrom);
          table.setStartFrom(startFrom);

        }
        checkJsonValues();


      } catch (Exception e) {
        LOG.error("Exception reading status file, doing back up and creating new status file", e);
        backupStatusFile();
      }
    }
  }

  private void checkJsonValues() throws ParseException {

  }

  private void backupStatusFile() {
    file.renameTo(new File(statusFilePath + "/" + statusFileName + ".bak." + System.currentTimeMillis()));
  }

  public void checkMandatoryProperties() {

    if (connectionURL == null) {
      throw new ConfigurationException("hibernate.connection.url property not set");
    }
    if (statusFileName == null) {
      throw new ConfigurationException("status.file.name property not set");
    }
    if (tables == null && customQuery == null) {
      throw new ConfigurationException("property table not set");
    }

    if (connectionUserName == null) {
      throw new ConfigurationException("hibernate.connection.user property not set");
    }

    if (connectionPassword == null) {
      throw new ConfigurationException("hibernate.connection.password property not set");
    }
  }

  /*
   * @return boolean pathname into directory
   */
  private boolean createDirectory() {
    return directory.mkdir();
  }

  /*
   * @return int delay in ms
   */
  int getRunQueryDelay() {
    return runQueryDelay;
  }

  int getBatchSize() {
    return batchSize;
  }

  int getMaxRows() {
    return maxRows;
  }

  String getQuery() {
    return query;
  }

  String getConnectionURL() {
    return connectionURL;
  }

  boolean isCustomQuerySet() {
    return (customQuery != null);
  }

  Context getContext() {
    return context;
  }

  boolean isReadOnlySession() {
    return readOnlySession;
  }

  boolean encloseByQuotes() {
    return encloseByQuotes;
  }

  String getDelimiterEntry() {
    return delimiterEntry;
  }

  public String getConnectionUserName() {
    return connectionUserName;
  }

  public String getConnectionPassword() {
    return connectionPassword;
  }

  public String getDefaultCharsetResultSet() {
    return defaultCharsetResultSet;
  }
}


class Table {

  private static final Logger LOG = LoggerFactory.getLogger(Table.class);
  private String name;
  private String iField;
  private String iFieldType;
  private Integer iFieldIndex;
  private Long startFrom;
  private String fields;

  private String fieldsShow;
  private List<String> staticFields = new LinkedList<>();
  private List<String> staticValues = new LinkedList<>();
  private Map<String, String> staticFieldsMap = null;
  public void setFields(String fields) {
    this.fields = fields;
  }

  public Map<String, String> getStaticFieldsMap() {
    if(staticFieldsMap == null) {
      staticFieldsMap = new HashMap<>();
      List<String> staticFieldNames = getStaticFields();
      if(staticFieldNames != null && staticFieldNames.size() > 0) {
        List<String> staticValues = getStaticValues();
        if(staticFieldNames.size() != staticValues.size()) {
          LOG.error(String.format("staticFields length must match staticValues length staticFields: %s staticValues:s%", staticFieldNames, staticValues));
          throw new RuntimeException("staticFields length must match staticValues length");
        }
        for(int i =  0; i < staticFieldNames.size(); i++) {
          staticFieldsMap.put(staticFieldNames.get(i), staticValues.get(i));
        }
      }
    }
    return staticFieldsMap;
  }

  public String getFields() {
    if(StringUtils.isEmpty(fields)) {
      return "*";
    }
    return fields;

  }



  public String getFieldsShow() {
    return fieldsShow;
  }

  public void setFieldsShow(String fieldsShow) {
    this.fieldsShow = fieldsShow;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getiField() {
    return iField;
  }

  public List<String> getStaticFields() {
    return staticFields;
  }

  public void setStaticFields(String staticFields) {
    if(StringUtils.isNotEmpty(staticFields)) {
      this.staticFields.clear();
      for(String field : staticFields.split(",")) {

        this.staticFields.add(field);

      }

    }

  }



  public List<String> getStaticValues() {
    return staticValues;
  }

  public void setStaticValues(String staticValues) {
    if(StringUtils.isNotEmpty(staticValues)) {
      this.staticValues.clear();
      for(String value : staticValues.split(",")) {

        this.staticValues.add(value);

      }

    }
  }

  public void setiField(String iField) {
    if(StringUtils.isNotEmpty(iField)) {
      this.iField = iField.toUpperCase();
    }
  }


  public String getiFieldType() {
    return iFieldType;
  }

  public void setiFieldType(String iFieldType) {
    this.iFieldType = iFieldType;
  }

  public Long getStartFrom() {
    LOG.debug("startFrom1:" + startFrom);
    if(this.startFrom != null ) {
      if("date".equalsIgnoreCase(this.getiFieldType()) && startFrom == 0) {
          this.startFrom = getiFieldDefaultValue();
      }
    }else {
      this.startFrom = getiFieldDefaultValue();
    }

    LOG.debug("startFrom2:" + startFrom);
    return this.startFrom;
  }

  public void setStartFrom(String startFrom) {
    if(StringUtils.isNotEmpty(startFrom)) {
      this.startFrom = Long.valueOf(startFrom);
    }
  }

  public Integer getiFieldIndex() {
    return iFieldIndex;
  }

  public void setiFieldIndex(String iFieldIndex) {

    this.iFieldIndex = Integer.valueOf(iFieldIndex);
  }

  private Long getiFieldDefaultValue() {

    if("date".equalsIgnoreCase(iFieldType)) {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
      String currStr = sdf.format(new Date());
      return Long.valueOf(currStr);
    }else if("number".equals(iFieldType)) {
      return -1l;
    }else {
      return 0l;
    }

  }

  public Long changeStartFrom(String startFrom) {
         LOG.debug("change before:" + startFrom);
         if(StringUtils.isNotEmpty(startFrom)) {
           startFrom = startFrom.replaceAll("[-:\\s]", "");
           if("date".equalsIgnoreCase(getiFieldType())) {
             if(startFrom.length() > 14) {
               startFrom = startFrom.substring(0,14);
             }
           }
           LOG.debug("change before2:" + startFrom);
           this.startFrom = Math.max(Long.valueOf(startFrom), this.getStartFrom());

         }
         LOG.debug("change after:" + this.startFrom);

         return this.startFrom;
  }
}