/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.infrastructure.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.fb_status;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.db.ColumnFamily;
import com.facebook.infrastructure.db.ColumnFamilyNotDefinedException;
import com.facebook.infrastructure.db.IColumn;
import com.facebook.infrastructure.db.ReadMessage;
import com.facebook.infrastructure.db.ReadResponseMessage;
import com.facebook.infrastructure.db.Row;
import com.facebook.infrastructure.db.RowMutation;
import com.facebook.infrastructure.db.RowMutationMessage;
import com.facebook.infrastructure.db.SuperColumn;
import com.facebook.infrastructure.db.Table;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.net.IAsyncResult;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.MessagingService;
import com.facebook.infrastructure.utils.LogUtil;
import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocolFactory;
import com.facebook.thrift.server.TThreadPoolServer;
import com.facebook.thrift.server.TThreadPoolServer.Options;
import com.facebook.thrift.transport.TServerSocket;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik (
 * pmalik@facebook.com )
 */

public class CassandraServer extends FacebookBase implements Cassandra.Iface {

  private static Logger logger_ = Logger.getLogger(CassandraServer.class);
  /*
   * Handle to the storage service to interact with the other machines in the
   * cluster.
   */
  StorageService storageService_;

  protected CassandraServer(String name) {
    super(name);
  }

  public CassandraServer() throws Throwable {
    super("Peerstorage");
    // Create the instance of the storage service
    storageService_ = StorageService.instance();
  }

  /*
   * The start function initializes the server and starts listening on the specified port
   */
  public void start() throws Throwable {
    LogUtil.init();
    // LogUtil.setLogLevel("com.facebook", "DEBUG");
    // Start the storage service
    storageService_.start();
  }

  private Map<EndPoint, Message> createWriteMessages(
      RowMutationMessage rmMessage, Map<EndPoint, EndPoint> endpointMap)
      throws IOException {
    Map<EndPoint, Message> messageMap = new HashMap<EndPoint, Message>();
    Message message = RowMutationMessage.makeRowMutationMessage(rmMessage);

    for (Map.Entry<EndPoint, EndPoint> entry : endpointMap.entrySet())
    {
      EndPoint target = entry.getKey();
      EndPoint hint = entry.getValue();

      if (!target.equals(hint)) {
        Message hintedMessage =
          RowMutationMessage.makeRowMutationMessage(rmMessage);
        hintedMessage.addHeader(RowMutationMessage.hint_,
                                EndPoint.toBytes(hint));
        logger_.debug(
          "Sending the hint of " + target.getHost() + " to " + hint.getHost());
        messageMap.put(target, hintedMessage);
      } else {
        messageMap.put(target, message);
      }
    }
    return messageMap;
  }

  protected void insert(RowMutation rm) {
    // 1. Get the N nodes from storage service where the data needs to be
    // replicated
    // 2. Construct a message for read\write
    // 3. SendRR ( to all the nodes above )
    // 4. Wait for a response from atleast X nodes where X <= N
    // 5. return success

    try {
      logger_.debug(" insert");
      Map<EndPoint, EndPoint> endpointMap =
        storageService_.getNStorageEndPointMap(rm.key());

      // TODO: throw a thrift exception if we do not have N nodes
      RowMutationMessage rmMsg = new RowMutationMessage(rm);

      /* Create the write messages to be sent */
      Map<EndPoint, Message> messageMap =
        createWriteMessages(rmMsg, endpointMap);

      for (Map.Entry<EndPoint, Message> entry : messageMap.entrySet()) {
        EndPoint endpoint = entry.getKey();

        MessagingService.getMessagingInstance().sendOneWay(entry.getValue(), endpoint);
      }
    } catch (Exception e) {
      logger_.info(LogUtil.throwableToString(e));
    }
    return;
  }

  /**
   * Performs the actual reading of a row out of the StorageService, fetching
   * a specific set of column names from a given column family.
   */
  protected Row readProtocol(String tablename,
                             String key,
                             String columnFamily,
                             IColumnSelection columnSelection,
                             StorageService.ConsistencyLevel consistencyLevel)
    throws TException
  {
    try {
      if(! storageService_.isInTopN(key) ||
         consistencyLevel == StorageService.ConsistencyLevel.STRONG)
      {
        return strongReadProtocol(tablename, key, columnFamily, columnSelection);
      }
      else
      {
        return weakReadProtocol(tablename, key, columnFamily, columnSelection);
      }
    }
    catch (ColumnFamilyNotDefinedException cfnde) {
      throw new InvalidColumnNameException(cfnde.getMessage());
    }
    catch (IOException ioe) {
      throw new TException(ioe.toString());
    }
    catch (TimeoutException toe) {
      throw new TException(toe.toString());
    }
  }
  
  /**
   * Perform a read using the "strong" protocol.
   */
  protected Row strongReadProtocol(String tablename,
                                   String key,
                                   String columnFamily,
                                   IColumnSelection columnSelection)
    throws IOException, TimeoutException, ColumnFamilyNotDefinedException
  {
    long startTime = System.currentTimeMillis();
    // TODO: throw a thrift exception if we do not have N nodes

    ReadMessage readMessage = columnSelection.makeReadMessage(tablename, key, columnFamily);
    ReadMessage readMessageDigestOnly = columnSelection.makeReadMessage(tablename, key, columnFamily);
    readMessageDigestOnly.setIsDigestQuery(true);

    Row row = doStrongReadProtocol(key, readMessage, readMessageDigestOnly);
    logger_.info("readProtocol: " + (System.currentTimeMillis() - startTime) + " ms.");
    return row;
  }

  /*
   * This method performs the actual read from the replicas.
   *
   * The strong consistency read works as follows:
   *
   *  - The list of nodes who have replicated the key is retrieved.
   *  - One of those nodes is designated as the data endpoint.
   *  - All of the other nodes are digest endpoints.
   *  - Requests are sent to all nodes in parallel. The actual row data
   *    is requested from the data endpoint; a digest of the data is
   *    requested from the other endpoints.
   *
   *
   *  param @ key - key for which the data is required.
   *  param @ readMessage - the read message to get the actual data
   *  param @ readMessageDigest - the read message to get the digest.
   */
  private Row doStrongReadProtocol(String key,
                                   ReadMessage readMessage,
                                   ReadMessage readMessageDigest) throws IOException, TimeoutException
  {
    Message message = ReadMessage.makeReadMessage(readMessage);
    Message messageDigestOnly = ReadMessage.makeReadMessage(readMessageDigest);

    // Pick the endpoint that will be the "data" endpoint. We let the
    // storage service pick this for us since it will prefer nearby
    // or local endpoints.
    EndPoint dataPoint = storageService_.findSuitableEndPoint(key);

    // Get the full list of endpoints to send to
    EndPoint[] endpoints = storageService_.getNStorageEndPoint(key);

    // Construct a parallel array of messages - message[i] is going to be
    // sent to endpoints[i] for all i.
    Message messages[] = new Message[endpoints.length];

    boolean didSetDataPoint = false;
    for (int i = 0; i < endpoints.length; i++) {
      if (endpoints[i].equals(dataPoint)) {
        messages[i] = message;
        didSetDataPoint = true;
      } else {
        messages[i] = messageDigestOnly;
      }
    }

    // We need to check for a race condition here - it's possible that between
    // getting the data endpoint and getting the list of endpoints, the endpoint list
    // might have been updated such that we are only going to send out digests. In that
    // case, fall back to just setting the first endpoint to be the data message.
    if (! didSetDataPoint )
      messages[0] = message;

    // Send the messages!
    try {
      IResponseResolver<Row> readResponseResolver = new ReadResponseResolver();
      QuorumResponseHandler<Row> quorumResponseHandler = new QuorumResponseHandler<Row>(
        DatabaseDescriptor.getReplicationFactor(),
        readResponseResolver);

      MessagingService.getMessagingInstance().sendRR(messages, endpoints,	quorumResponseHandler);

      long startTime2 = System.currentTimeMillis();
      Row row = quorumResponseHandler.get();
      logger_.info("quorumResponseHandler: " + (System.currentTimeMillis() - startTime2)
                   + " ms.");

      if (row == null) {
        logger_.info("ERROR No row for this key .....: " + key);
        // TODO: throw a thrift exception
      }

      return row;

    } catch (DigestMismatchException ex) {
      // The read resolver detected that at least one of the nodes had data that differed
      // from the returned data. In order to repair, we request the full data from all nodes.
      IResponseResolver<Row> readResponseResolverRepair = new ReadResponseResolver();
      QuorumResponseHandler<Row> quorumResponseHandlerRepair = new QuorumResponseHandler<Row>(
        DatabaseDescriptor.getReplicationFactor(),
        readResponseResolverRepair);
      readMessage.setIsDigestQuery(false);
      logger_.info("DigestMismatchException: " + key);

      // Make a new message so that our repair read has a new ID. Otherwise stale messages might
      // hit us and confuse things.
      Message messageRepair = ReadMessage.makeReadMessage(readMessage);
      MessagingService.getMessagingInstance().sendRR(messageRepair, endpoints,
                                                     quorumResponseHandlerRepair);

      Row row = null;
      try {
        row = quorumResponseHandlerRepair.get();
      } catch(DigestMismatchException dex) {
        // TODO is there any way to get here? There should be no digests in the
        // read resolve process, so this exception shouldn't be possible.
        logger_.warn(LogUtil.throwableToString(dex));
      }
      if (row == null) {
        logger_.info("ERROR No row for this key .....: " + key);
      }
      return row;
    }
  }

  /*
   * This function executes the read protocol locally and should be used only if consistency is not a concern.
   * Read the data from the local disk and return if the row is NOT NULL. If the data is NULL do the read from
   * one of the other replicas (in the same data center if possible) till we get the data. In the event we get
   * the data we perform consistency checks and figure out if any repairs need to be done to the replicas.
   *
   * TODO: this comment does not seem to reflect what the code actually does...
   */
  protected Row weakReadProtocol(String tablename,
                                 String key,
                                 String columnFamily,
                                 IColumnSelection columnSelection)
    throws IOException, TimeoutException, ColumnFamilyNotDefinedException
  {
    long startTime = System.currentTimeMillis();
    List<EndPoint> endpoints = storageService_.getNLiveStorageEndPoint(key);

    logger_.debug("Endpoints: ");
    for (EndPoint ep : endpoints) {
      logger_.debug("   " + String.valueOf(ep));
    }

    /* Remove the local storage endpoint from the list. */
    endpoints.remove( StorageService.getLocalStorageEndPoint() );
    // TODO: throw a thrift exception if we do not have N nodes

    Table table = Table.open( tablename );
    Row row = columnSelection.getTableRow(table, key, columnFamily);

    logger_.info("Local Read Protocol: " + (System.currentTimeMillis() - startTime) + " ms.");
    /*
     * Do the consistency checks in the background and return the
     * non NULL row.
     */
    if (!endpoints.isEmpty()) {
      columnSelection.doConsistencyCheck(
        StorageService.instance(), row, endpoints, columnFamily);
    }
    return row;
  }

  /**
   * Gets the ColumnFamily object for the given table, key, and cf.
   *
   * Throws Thrift Exceptions for missing column family, table, etc.
   * Will not return null.
   */
  protected ColumnFamily get_cf(String tablename,
                                String key,
                                String columnFamily,
                                IColumnSelection columnSelection)
    throws TException
  {	
    Row row = readProtocol(tablename, key, columnFamily, columnSelection,
                           StorageService.ConsistencyLevel.WEAK);
    if (row == null)
      throw new DataNotFoundException("No row for key: " + key);
        
    ColumnFamily cfamily = row.getColumnFamily( columnFamily );
    if (cfamily == null)
      throw new DataNotFoundException("ColumnFamily " + columnFamily + " is missing.....: "
                                      + "  key:" + key
                                      + "  ColumnFamily:" + columnFamily);
    return cfamily;
  }


  /**
   * Returns the collection of columns referred to by columnFamily_column.
   * This parameter may either be just the string "cfname" or it may be
   * "cfname:supercolumn".
   * If the column is not found, currently returns an empty list. This
   * may be changed to an exception in the future.
   */
  private Collection<IColumn> getColumns(String tablename,
                                         String key,
                                         String columnFamily_column,
                                         IColumnSelection columnSelection)
    throws TException
  {
    String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);

    ColumnFamily cfamily = get_cf(tablename, key, values[0], columnSelection);

    if( values.length > 1 )
    {
      // this is the super column case
      IColumn column = cfamily.getColumn(values[1]);
      if(column != null)
        return column.getSubColumns();
    }
    else
    {
      return cfamily.getAllColumns();

    }

    throw new DataNotFoundException(
      "ERROR Columns are missing.....: "
      + "   key:" + key
      + "  ColumnFamily:" + values[0]);
  }


  public ArrayList<column_t> get_columns_since(
    String tablename, String key, String columnFamily_column, long timeStamp)
    throws TException
  {
    return getThriftColumns(tablename, key, columnFamily_column,
                            new SinceTimestampSelection(timeStamp));
  }


  public ArrayList<column_t> get_slice(String tablename,
                                       String key,
                                       String columnFamily_column,
                                       int start,
                                       int count) throws TException
  {
    return getThriftColumns(tablename, key, columnFamily_column,
                            makeSliceSelection(start, count));
  }


  /**
   * Used to implement other thrift methods
   */
  private ArrayList<column_t> getThriftColumns(
    String tablename, String key,
    String columnFamily_column,
    IColumnSelection columnSelection) throws TException
  {

    ArrayList<column_t> retlist = new ArrayList<column_t>();
    Collection<IColumn> cols = getColumns(tablename, key, columnFamily_column, columnSelection);
        
    for(IColumn column : cols) {
      retlist.add(makeThriftColumn(column));
    }

    return retlist;
  }

  /**
   * Get the data in a single cell.
   */
  public column_t get_column(String tablename, String key, String columnFamily_column) throws TException
  {
    // Check format of column argument
    String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);

    if (values.length < 2)
      throw new InvalidColumnNameException(
        "get_column expects either 'columnFamily:column' or 'columnFamily:superCol:col'");

    String columnFamilyName = values[0];
    String columnName = values[1];

    IColumnSelection sel = new ColumnListSelection(Arrays.asList(new String[] { columnName }));
    ColumnFamily cfamily = get_cf(tablename, key, columnFamilyName, sel);

    IColumn col = cfamily.getColumn(columnName);

    // Handle supercolumn fetches
    if (col != null &&
        values.length == 3) {
      // They want a column within a supercolumn
      try
      {
        SuperColumn sc = (SuperColumn)col;
        col = sc.getSubColumn(values[2]);
      }
      catch (ClassCastException cce)
      {
        throw new InvalidColumnNameException(
          "Column " + values[1] + " is not a supercolumn.");
      }
    }

    if (col == null)
      throw new DataNotFoundException("ERROR Columns is missing.....: "
                                      + "   key:" + key
                                      + "  ColumnFamily:" + values[0]
                                      + "   col: " + values[1]);

    return makeThriftColumn(col);
  }

  /**
   * Return the number of columns in a specified column/supercolumn.
   * If a column is referenced this will be 1. Otherwise it will be the
   * number of columns in the supercolumn.
   */
  public int get_column_count(String tablename, String key, String columnFamily_column)
    throws TException
  {
    String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
    // check for  values
    if( values.length < 1 )
      return -1;

    IColumnSelection selection = new AllColumnSelection();
    Collection<IColumn> columns = getColumns(tablename, key, columnFamily_column, selection);

    return columns.size();
  }

  public void insert(String tablename, String key, String columnFamily_column, String cellData, long timestamp)
  {
    try
    {
      RowMutation rm = new RowMutation(tablename, key.trim());
      rm.add(columnFamily_column, cellData.getBytes(), timestamp);
      insert(rm);
    }
    catch (Exception e)
    {
      logger_.debug( LogUtil.throwableToString(e) );
    }
    return;
  }

  public void insert_blocking(String tablename, String key, String columnFamily_column, String cellData, int timestamp)
  {
    insert(tablename, key, columnFamily_column, cellData, timestamp);
    // TODO: return some kind of status from this call
  }

  public boolean batch_insert_blocking(batch_mutation_t batchMutation)
  {
    // 1. Get the N nodes from storage service where the data needs to be
    // replicated
    // 2. Construct a message for read\write
    // 3. SendRR ( to all the nodes above )
    // 4. Wait for a response from at least X nodes where X <= N
    // 5. return success
    boolean result = false;
    try
    {
      logger_.warn(" batch_insert_blocking");
      IResponseResolver<Boolean> writeResponseResolver = new WriteResponseResolver();
      QuorumResponseHandler<Boolean> quorumResponseHandler = new QuorumResponseHandler<Boolean>(
        DatabaseDescriptor.getReplicationFactor(),
        writeResponseResolver);
      EndPoint[] endpoints = storageService_.getNStorageEndPoint(batchMutation.key);
      // TODO: throw a thrift exception if we do not have N nodes

      logger_.debug(" Creating the row mutation");
      RowMutation rm = new RowMutation(batchMutation.table,
                                       batchMutation.key.trim());

      for ( Map.Entry<String, List<column_t>> entry : batchMutation.cfmap.entrySet() )
      {
        String key = entry.getKey();
        List<column_t> list = entry.getValue();

        for (column_t columnData : list)
        {
          rm.add(key + ":" + columnData.columnName,
                 columnData.value.getBytes(), columnData.timestamp);
        }
      }

      RowMutationMessage rmMsg = new RowMutationMessage(rm);
      Message message = new Message(StorageService.getLocalStorageEndPoint(),
                                    StorageService.mutationStage_,
                                    StorageService.mutationVerbHandler_,
                                    new Object[]{ rmMsg }
        );
      MessagingService.getMessagingInstance().sendRR(message, endpoints,
                                                     quorumResponseHandler);
      logger_.debug(" Calling quorum response handler's get");
      result = quorumResponseHandler.get();

      // TODO: if the result is false that means the writes to all the
      // servers failed hence we need to throw an exception or return an
      // error back to the client so that it can take appropriate action.
    }
    catch (Exception e)
    {
      logger_.info( LogUtil.throwableToString(e) );
    }
    return result;

  }
  public void batch_insert(batch_mutation_t batchMutation)
  {
    // 1. Get the N nodes from storage service where the data needs to be
    // replicated
    // 2. Construct a message for read\write
    // 3. SendRR ( to all the nodes above )
    // 4. Wait for a response from atleast X nodes where X <= N
    // 5. return success

    try
    {
      logger_.debug(" batch_insert");
      logger_.debug(" Creating the row mutation");
      RowMutation rm = new RowMutation(batchMutation.table,
                                       batchMutation.key.trim());

      for ( Map.Entry<String, List<column_t>> entry : batchMutation.cfmap.entrySet() )
      {
        String key = entry.getKey();
        List<column_t> list = entry.getValue();
        for (column_t columnData : list)
        {
          rm.add(key + ":" + columnData.columnName,
                 columnData.value.getBytes(), columnData.timestamp);

        }
      }
      insert(rm);
    }
    catch (Exception e)
    {
      logger_.info( LogUtil.throwableToString(e) );
    }
    return;
  }

  public void remove(String tablename, String key, String columnFamily_column)
  {
    try
    {
      RowMutation rm = new RowMutation(tablename, key.trim());
      rm.delete(columnFamily_column);
      insert(rm);
    }
    catch (Exception e)
    {
      logger_.debug( LogUtil.throwableToString(e) );
    }
    return;
  }

  public ArrayList<superColumn_t> get_slice_super(String tablename,
                                                  String key,
                                                  String columnFamily_superColumnName,
                                                  int start,
                                                  int count)
    throws TException
  {
    ArrayList<superColumn_t> retlist = new ArrayList<superColumn_t>();

    IColumnSelection selection = makeSliceSelection(start, count);


    Collection<IColumn> columns = getColumns(tablename, key, columnFamily_superColumnName, selection);

    for(IColumn column : columns)
    {
      retlist.add(makeThriftSuperColumn(column));
    }
    return retlist;
  }

  public superColumn_t get_superColumn(String tablename, String key, String columnFamily_column)
    throws TException
  {
    String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
    if (values.length != 2)
      throw new InvalidColumnNameException("get_superColumn expects column of form cfamily:supercol");

    ColumnFamily cfamily = get_cf(tablename, key, values[0], new AllColumnSelection());
    IColumn col = cfamily.getColumn(values[1]);
    if (col == null)
    {
      throw new DataNotFoundException("Couldn't find column " + values[1] + " in row " + key);
    }

    return makeThriftSuperColumn(col);
  }

  public boolean batch_insert_superColumn_blocking(batch_mutation_super_t batchMutationSuper)
  {
    boolean result = false;
    try
    {
      logger_.warn(" batch_insert_SuperColumn_blocking");
      logger_.debug(" Creating the row mutation");
      RowMutation rm = new RowMutation(batchMutationSuper.table,
                                       batchMutationSuper.key.trim());

      for ( Map.Entry<String, List<superColumn_t>> entry : batchMutationSuper.cfmap.entrySet() )
      {
        String key = entry.getKey();
        List<superColumn_t> list = entry.getValue();
        for (superColumn_t superColumnData : list)
        {
          if (!superColumnData.columns.isEmpty() )
          {
            for (column_t columnData : superColumnData.columns)
            {
              rm.add(key + ":" + superColumnData.name  +":" + columnData.columnName,
                     columnData.value.getBytes(), columnData.timestamp);
            }
          }
          else
          {
            rm.add(key + ":" + superColumnData.name, new byte[0], 0);
          }
        }
      }
      insert(rm);
    }
    catch (Exception e)
    {
      logger_.info( LogUtil.throwableToString(e) );
    }
    return result;

  }
  public void batch_insert_superColumn(batch_mutation_super_t batchMutationSuper)
  {
    try
    {
      logger_.debug(" batch_insert");
      logger_.debug(" Creating the row mutation");
      RowMutation rm = new RowMutation(batchMutationSuper.table,
                                       batchMutationSuper.key.trim());

      for ( Map.Entry<String, List<superColumn_t>> entry : batchMutationSuper.cfmap.entrySet() )
      {
        String key = entry.getKey();
        List<superColumn_t> list = entry.getValue();
        for (superColumn_t superColumnData : list)
        {
          if ( !superColumnData.columns.isEmpty() )
          {
            for (column_t columnData : superColumnData.columns)
            {
              rm.add(key + ":" + superColumnData.name  +":" + columnData.columnName,
                     columnData.value.getBytes(), columnData.timestamp);
            }
          }
          else
          {
            rm.add(key + ":" + superColumnData.name, new byte[0], 0);
          }
        }
      }
      insert(rm);
    }
    catch (Exception e)
    {
      logger_.info( LogUtil.throwableToString(e) );
    }
    return;
  }

  /**
   * Convert a Java IColumn into a column_t suitable for returning
   */
  private column_t makeThriftColumn(IColumn column) {
    column_t thrift_column = new column_t();
    thrift_column.columnName = column.name();
    thrift_column.value = new String(column.value()); // This needs to be Utf8ed
    thrift_column.timestamp = column.timestamp();
    return thrift_column;
  }

  private superColumn_t makeThriftSuperColumn(IColumn column) {
    superColumn_t ret = new superColumn_t();
    ret.name = column.name();
    Collection<IColumn> subColumns = column.getSubColumns();
    ret.columns = new ArrayList<column_t>();
    for(IColumn subColumn : subColumns)
    {                    
      ret.columns.add(makeThriftColumn(subColumn));
    }
    return ret;
  }


  /**
   * Makes an IColumnSelection implementation for a given start/count parameter.
   */
  private static IColumnSelection makeSliceSelection(int start, int count)
  {
    if( start >= 0 && count < Integer.MAX_VALUE)
      return new ColumnRangeSelection(start, count);
    else
      return new AllColumnSelection();
  }

  /**
   * Specifies a slice of a row's columns.
   * The implementing classes are directly below.
   *
   * TODO: this should probably be factored out of the thrift interface
   * and generally used as a convenient structure in Table.java, etc
   */
  private static interface IColumnSelection {
    public ReadMessage makeReadMessage(String tableName, String key, String columnfamily);
    public Row getTableRow(Table table, String key, String columnFamily)
      throws ColumnFamilyNotDefinedException, IOException;
    public void doConsistencyCheck(StorageService service, Row row, List<EndPoint> endpoints, String columnFamily);
  }


  /**
   * Selects a set of columns by numeric indices
   */
  private static class ColumnRangeSelection implements IColumnSelection
  {
    private final int start_, count_;
    public ColumnRangeSelection(int start, int count)
    {
      start_ = start;
      count_ = count;
    }

    public ReadMessage makeReadMessage(String tableName, String key, String columnFamily)
    {
      return new ReadMessage(tableName, key, columnFamily, start_, count_);
    }

    public Row getTableRow(Table table, String key, String columnFamily)
      throws ColumnFamilyNotDefinedException, IOException
    {
      return table.getRow(key, columnFamily, start_, count_);
    }

    public void doConsistencyCheck(StorageService service, Row row, List<EndPoint> endpoints, String columnFamily)
    {
      service.doConsistencyCheck(row, endpoints, columnFamily, start_, count_);
    }
  }

  /**
   * Selects columns that were written after a given timestamp
   */
  private static class SinceTimestampSelection implements IColumnSelection
  {
    private final long sinceTimestamp_;

    public SinceTimestampSelection(long timestamp) {
      sinceTimestamp_ = timestamp;
    }

    public ReadMessage makeReadMessage(String tableName, String key, String columnFamily) {
      return new ReadMessage(tableName, key, columnFamily, sinceTimestamp_);
    }

    public Row getTableRow(Table table, String key, String columnFamily)
      throws ColumnFamilyNotDefinedException, IOException
    {
      return table.getRow(key, columnFamily, sinceTimestamp_);
    }

    public void doConsistencyCheck(StorageService service, Row row, List<EndPoint> endpoints, String columnFamily)
    {
      service.doConsistencyCheck(row, endpoints, columnFamily, sinceTimestamp_);
    }
  }

  /**
   * Selects all columns in a row
   */
  private static class AllColumnSelection implements IColumnSelection
  {
    public ReadMessage makeReadMessage(String tableName, String key, String columnFamily)
    {
      return new ReadMessage(tableName, key, columnFamily);
    }        

    public Row getTableRow(Table table, String key, String columnFamily)
      throws ColumnFamilyNotDefinedException, IOException
    {
      return table.getRow(key, columnFamily);
    }

    public void doConsistencyCheck(StorageService service, Row row, List<EndPoint> endpoints, String columnFamily)
    {
      service.doConsistencyCheck(row, endpoints, columnFamily, -1, -1);
    }
  }

  /**
   * Selects a given list of columns (by string column names)
   */
  private static class ColumnListSelection implements IColumnSelection
  {
    private final List<String> columns_;

    public ColumnListSelection(List<String> columns)
    {
      columns_ = columns;
    }
            
    public ReadMessage makeReadMessage(String tableName, String key, String columnFamily)
    {
      return new ReadMessage(tableName, key, columnFamily, columns_);
    }

    public Row getTableRow(Table table, String key, String columnFamily)
      throws ColumnFamilyNotDefinedException, IOException
    {
      return table.getRow(key, columnFamily, columns_);
    }

    public void doConsistencyCheck(StorageService service, Row row, List<EndPoint> endpoints, String columnFamily)
    {
      service.doConsistencyCheck(row, endpoints, columnFamily, columns_);
    }
  }

  private static class InvalidColumnNameException extends TException {
    InvalidColumnNameException(String s) { super(s); }
  }
  private static class DataNotFoundException extends TException {
    DataNotFoundException(String s) { super(s); }
  }

  public String getVersion() {
    return "1";
  }

  public int getStatus() {
    return fb_status.ALIVE;
  }

  public String getStatusDetails() {
    return null;
  }

  public static void main(String[] args) throws Throwable {
    int port = 9160;
    try {
      CassandraServer peerStorageServer = new CassandraServer();
      peerStorageServer.start();
      Cassandra.Processor processor = new Cassandra.Processor(peerStorageServer);
      // Transport
      TServerSocket tServerSocket = new TServerSocket(port);
      // Protocol factory
      TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory();
      // ThreadPool Server
      Options options = new Options();
      options.minWorkerThreads = 64;
      TThreadPoolServer serverEngine = new TThreadPoolServer(processor,
          tServerSocket, tProtocolFactory);
      serverEngine.serve();

    } catch (Exception x) {
      System.err.println("UNCAUGHT EXCEPTION IN main()");
      x.printStackTrace();
      System.exit(1);
    }

  }

}
