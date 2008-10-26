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

package com.facebook.infrastructure.db;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.dht.Range;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.io.DataOutputBuffer;
import com.facebook.infrastructure.io.IFileReader;
import com.facebook.infrastructure.io.IndexHelper;
import com.facebook.infrastructure.io.SSTable;
import com.facebook.infrastructure.io.SequenceFile;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.service.StorageService;
import com.facebook.infrastructure.utils.BloomFilter;
import com.facebook.infrastructure.utils.LogUtil;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik (
 * pmalik@facebook.com )
 */

public class ColumnFamilyStore {
  static class FileStructComparator implements Comparator<FileStruct> {
    public int compare(FileStruct f, FileStruct f2) {
      return f.reader.getFileName().compareTo(f2.reader.getFileName());
    }

    public boolean equals(Object o) {
      if (!(o instanceof FileStructComparator))
        return false;
      return true;
    }
  }

  static class FileNameComparator implements Comparator<String> {
    public int compare(String f, String f2) {
      return getIndexFromFileName(f2) - getIndexFromFileName(f);
    }

    public boolean equals(Object o) {
      if (!(o instanceof FileNameComparator))
        return false;
      return true;
    }
  }

  class FileStruct implements Comparable<FileStruct> {
    IFileReader reader;
    String key;
    DataInputBuffer bufIn;
    DataOutputBuffer bufOut;

    public int compareTo(FileStruct f) {
      return key.compareTo(f.key);
    }
  }

  private static int threshHold_ = 4;
  private static final int bufSize_ = 128 * 1024 * 1024;
  private static int compactionMemoryThreshold_ = 1 << 30;
  private static Logger logger_ = Logger.getLogger(ColumnFamilyStore.class);

  private String table_;
  public String columnFamily_;

  /* This is used to generate the next index for a SSTable */
  private AtomicInteger fileIndexGenerator_ = new AtomicInteger(0);

  /* memtable associated with this ColumnFamilyStore. */
  private AtomicReference<Memtable> memtable_;
  private AtomicReference<BinaryMemtable> binaryMemtable_;

  /* SSTables on disk for this column family */
  private Set<String> ssTables_ = new HashSet<String>();

  /* Modification lock used for protecting reads from compactions. */
  private ReentrantReadWriteLock lock_ = new ReentrantReadWriteLock(true);

  /* Flag indicates if a compaction is in process */
  public AtomicBoolean isCompacting_ = new AtomicBoolean(false);

  ColumnFamilyStore(String table, String columnFamily) throws IOException {
    table_ = table;
    columnFamily_ = columnFamily;
    /*
     * Get all data files associated with old Memtables for this table. These
     * files are named as follows <Table>-1.db, ..., <Table>-n.db. Get the max
     * which in this case is n and increment it to use it for next index.
     */
    List<Integer> indices = new ArrayList<Integer>();
    String[] dataFileDirectories = DatabaseDescriptor.getAllDataFileLocations();
    for (String directory : dataFileDirectories) {
      File fileDir = new File(directory);
      File[] files = fileDir.listFiles();
      for (File file : files) {
        String filename = file.getName();
        String[] tblCfName = getTableAndColumnFamilyName(filename);
        if (tblCfName[0].equals(table_) && tblCfName[1].equals(columnFamily)) {
          int index = getIndexFromFileName(filename);
          indices.add(index);
        }
      }
    }
    Collections.sort(indices);
    int value = (indices.size() > 0) ? (indices.get(indices.size() - 1)) : 0;
    fileIndexGenerator_.set(value);
    memtable_ = new AtomicReference<Memtable>(new Memtable(table_,
        columnFamily_));
    binaryMemtable_ = new AtomicReference<BinaryMemtable>(new BinaryMemtable(
        table_, columnFamily_));
  }

  void onStart() throws IOException {
    /* Do major compaction */
    List<File> ssTables = new ArrayList<File>();
    String[] dataFileDirectories = DatabaseDescriptor.getAllDataFileLocations();
    for (String directory : dataFileDirectories) {
      File fileDir = new File(directory);
      File[] files = fileDir.listFiles();
      for (File file : files) {
        String filename = file.getName();
        if (((file.length() == 0) || (filename.indexOf("-"
            + SSTable.temporaryFile_) != -1))
            && (filename.indexOf(columnFamily_) != -1)) {
          file.delete();
          continue;
        }
        String[] tblCfName = getTableAndColumnFamilyName(filename);
        if (tblCfName[0].equals(table_) && tblCfName[1].equals(columnFamily_)
            && filename.indexOf("-Data.db") != -1) {
          ssTables.add(file.getAbsoluteFile());
        }
      }
    }
    Collections.sort(ssTables, new FileUtils.FileComparator());
    List<String> filenames = new ArrayList<String>();
    for (File ssTable : ssTables) {
      filenames.add(ssTable.getAbsolutePath());
    }

    /* There are no files to compact just add to the list of SSTables */
    ssTables_.addAll(filenames);
    /* Load the index files and the Bloom Filters associated with them. */
    SSTable.onStart(filenames);
    logger_.debug("Submitting a major compaction task ...");
    MinorCompactionManager.instance().submit(ColumnFamilyStore.this);
    if (columnFamily_.equals(Table.hints_)) {
      HintedHandOffManager.instance().submit(this);
    }
    MinorCompactionManager.instance().submitPeriodicCompaction(this);
  }

  List<String> getAllSSTablesOnDisk() {
    return new ArrayList<String>(ssTables_);
  }

  /*
   * This method is called to obtain statistics about the Column Family
   * represented by this Column Family Store. It will report the total number of
   * files on disk and the total space oocupied by the data files associated
   * with this Column Family.
   */
  public String cfStats(String newLineSeparator, java.text.DecimalFormat df) {
    StringBuilder sb = new StringBuilder();
    /*
     * We want to do this so that if there are no files on disk we do not want
     * to display something ugly on the admin page.
     */
    if (ssTables_.size() == 0) {
      return sb.toString();
    }
    sb.append(columnFamily_ + " statistics :");
    sb.append(newLineSeparator);
    sb.append("Number of files on disk : " + ssTables_.size());
    sb.append(newLineSeparator);
    double totalSpace = 0d;
    for (String file : ssTables_) {
      File f = new File(file);
      totalSpace += f.length();
    }
    String diskSpace = FileUtils.stringifyFileSize(totalSpace);
    sb.append("Total disk space : " + diskSpace);
    sb.append(newLineSeparator);
    sb.append("--------------------------------------");
    sb.append(newLineSeparator);
    return sb.toString();
  }

  /*
   * This is called after bootstrap to add the files to the list of files
   * maintained.
   */
  void addToList(String file) {
    lock_.writeLock().lock();
    try {
      ssTables_.add(file);
    } finally {
      lock_.writeLock().unlock();
    }
  }

  void touch(String key, boolean fData) throws IOException {
    /* Scan the SSTables on disk first */
    lock_.readLock().lock();
    try {
      List<String> files = new ArrayList<String>(ssTables_);
      for (String file : files) {
        /*
         * Get the BloomFilter associated with this file. Check if the key is
         * present in the BloomFilter. If not continue to the next file.
         */
        boolean bVal = SSTable.isKeyInFile(key, file);
        if (!bVal)
          continue;
        SSTable ssTable = new SSTable(file);
        ssTable.touch(key, fData);
      }
    } finally {
      lock_.readLock().unlock();
    }
  }

  /*
   * This method forces a compaction of the SSTables on disk. We wait for the
   * process to complete by waiting on a future pointer.
   */
  BloomFilter.CountingBloomFilter forceCompaction(List<Range> ranges,
      EndPoint target, long skip, List<String> fileList) {
    BloomFilter.CountingBloomFilter cbf = null;
    Future<BloomFilter.CountingBloomFilter> futurePtr = null;
    if (ranges != null)
      futurePtr = MinorCompactionManager.instance().submit(
          ColumnFamilyStore.this, ranges, target, fileList);
    else
      futurePtr = MinorCompactionManager.instance().submitMajor(
          ColumnFamilyStore.this, ranges, skip);

    try {
      /* Waiting for the compaction to complete. */
      cbf = futurePtr.get();
      logger_.debug("Done forcing compaction ...");
    } catch (ExecutionException ex) {
      logger_.debug(LogUtil.throwableToString(ex));
    } catch (InterruptedException ex2) {
      logger_.debug(LogUtil.throwableToString(ex2));
    }
    return cbf;
  }

  String getColumnFamilyName() {
    return columnFamily_;
  }

  private String[] getTableAndColumnFamilyName(String filename) {
    StringTokenizer st = new StringTokenizer(filename, "-");
    String[] values = new String[2];
    int i = 0;
    while (st.hasMoreElements()) {
      if (i == 0)
        values[i] = (String) st.nextElement();
      else if (i == 1) {
        values[i] = (String) st.nextElement();
        break;
      }
      ++i;
    }
    return values;
  }

  protected static int getIndexFromFileName(String filename) {
    /*
     * File name is of the form <table>-<column family>-<index>-Data.db. This
     * tokenizer will strip the .db portion.
     */
    StringTokenizer st = new StringTokenizer(filename, "-");
    /*
     * Now I want to get the index portion of the filename. We accumulate the
     * indices and then sort them to get the max index.
     */
    int count = st.countTokens();
    int i = 0;
    String index = null;
    while (st.hasMoreElements()) {
      index = (String) st.nextElement();
      if (i == (count - 2))
        break;
      ++i;
    }
    return Integer.parseInt(index);
  }

  String getNextFileName() {
    String name = table_ + "-" + columnFamily_ + "-"
        + fileIndexGenerator_.incrementAndGet();
    return name;
  }

  /*
   * Return a temporary file name.
   */
  String getTempFileName() {
    String name = table_ + "-" + columnFamily_ + "-" + SSTable.temporaryFile_
        + "-" + fileIndexGenerator_.incrementAndGet();
    return name;
  }

  /*
   * This version is used only on start up when we are recovering from logs. In
   * the future we may want to parellelize the log processing for a table by
   * having a thread per log file present for recovery. Re-visit at that time.
   */
  void switchMemtable(String key, ColumnFamily columnFamily,
      CommitLog.CommitLogContext cLogCtx) throws IOException {
    memtable_.set(new Memtable(table_, columnFamily_));
    if (!key.equals(Memtable.flushKey_))
      memtable_.get().put(key, columnFamily, cLogCtx);
  }

  /*
   * This version is used when we forceflush.
   */
  void switchMemtable() throws IOException {
    memtable_.set(new Memtable(table_, columnFamily_));
  }

  /*
   * This version is used only on start up when we are recovering from logs. In
   * the future we may want to parellelize the log processing for a table by
   * having a thread per log file present for recovery. Re-visit at that time.
   */
  void switchBinaryMemtable(String key, byte[] buffer) throws IOException {
    binaryMemtable_.set(new BinaryMemtable(table_, columnFamily_));
    binaryMemtable_.get().put(key, buffer);
  }

  void forceFlush(boolean fRecovery) throws IOException {
    // MemtableManager.instance().submit(getColumnFamilyName(), memtable_.get()
    // , CommitLog.CommitLogContext.NULL);
    // memtable_.get().flush(true, CommitLog.CommitLogContext.NULL);
    memtable_.get().forceflush(this, fRecovery);
  }

  void forceFlushBinary() throws IOException {
    BinaryMemtableManager.instance().submit(getColumnFamilyName(),
        binaryMemtable_.get());
    // binaryMemtable_.get().flush(true);
  }

  /*
   * Insert/Update the column family for this key. param @ lock - lock that
   * needs to be used. param @ key - key for update/insert param @ columnFamily
   * - columnFamily changes
   */
  void apply(String key, ColumnFamily columnFamily,
      CommitLog.CommitLogContext cLogCtx) throws IOException {
    memtable_.get().put(key, columnFamily, cLogCtx);
  }

  /*
   * Insert/Update the column family for this key. param @ lock - lock that
   * needs to be used. param @ key - key for update/insert param @ columnFamily
   * - columnFamily changes
   */
  void applyBinary(String key, byte[] buffer) throws IOException {
    binaryMemtable_.get().put(key, buffer);
  }

  /**
   * 
   * Get the column family in the most efficient order. 1. Memtable 2. Sorted
   * list of files
   */
  public ColumnFamily getColumnFamily(String key, String cf, IFilter filter)
      throws IOException {
    List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();
    ColumnFamily columnFamily = null;
    long start = System.currentTimeMillis();
    /* Get the ColumnFamily from Memtable */
    getColumnFamilyFromCurrentMemtable(key, cf, filter, columnFamilies);
    if (columnFamilies.size() != 0) {
      if (filter.isDone())
        return columnFamilies.get(0);
    }
    /* Check if MemtableManager has any historical information */
    MemtableManager.instance().getColumnFamily(key, columnFamily_, cf, filter,
        columnFamilies);
    if (columnFamilies.size() != 0) {
      columnFamily = resolve(columnFamilies);
      if (filter.isDone())
        return columnFamily;
      columnFamilies.clear();
      columnFamilies.add(columnFamily);
    }
    getColumnFamilyFromDisk(key, cf, columnFamilies, filter);
    logger_.info("DISK TIME: " + (System.currentTimeMillis() - start) + " ms.");
    columnFamily = resolve(columnFamilies);

    return columnFamily;
  }

  /**
   * Fetch from disk files and go in sorted order to be efficient This fn exits
   * as soon as the required data is found.
   * 
   * @param key
   * @param cf
   * @param columnFamilies
   * @param filter
   * @throws IOException
   */
  private void getColumnFamilyFromDisk(String key, String cf,
      List<ColumnFamily> columnFamilies, IFilter filter) throws IOException {
    /* Scan the SSTables on disk first */
    lock_.readLock().lock();
    try {
      List<String> files = new ArrayList<String>(ssTables_);
      Collections.sort(files, new FileNameComparator());
      for (String file : files) {
        /*
         * Get the BloomFilter associated with this file. Check if the key is
         * present in the BloomFilter. If not continue to the next file.
         */
        boolean bVal = SSTable.isKeyInFile(key, file);
        if (!bVal)
          continue;
        ColumnFamily columnFamily = fetchColumnFamily(key, cf, filter, file);
        long start = System.currentTimeMillis();
        if (columnFamily != null) {
          /*
           * TODO By using the filter before removing deleted columns we have a
           * efficient implementation of timefilter but for count filter this
           * can return wrong results we need to take care of that later.
           */
          /* suppress columns marked for delete */
          Map<String, IColumn> columns = columnFamily.getColumnMap();
          Set<String> cNames = columns.keySet();

          for (String cName : cNames) {
            IColumn column = columns.get(cName);
            if (column.isMarkedForDelete())
              columns.remove(cName);
          }
          columnFamilies.add(columnFamily);
          if (filter.isDone()) {
            break;
          }
        }
        logger_.info("DISK Data structure population  TIME: "
            + (System.currentTimeMillis() - start) + " ms.");
      }
      files.clear();
    } finally {
      lock_.readLock().unlock();
    }
  }

  private ColumnFamily fetchColumnFamily(String key, String cf, IFilter filter,
      String ssTableFile) throws IOException {
    SSTable ssTable = new SSTable(ssTableFile);
    long start = System.currentTimeMillis();
    DataInputBuffer bufIn = null;
    bufIn = filter.next(key, cf, ssTable);
    logger_.info("DISK ssTable.next TIME: "
        + (System.currentTimeMillis() - start) + " ms.");
    if (bufIn.getLength() == 0)
      return null;
    start = System.currentTimeMillis();
    ColumnFamily columnFamily = null;
    columnFamily = ColumnFamily.serializer().deserialize(bufIn, cf, filter);
    logger_.info("DISK Deserialize TIME: "
        + (System.currentTimeMillis() - start) + " ms.");
    if (columnFamily == null)
      return columnFamily;
    return (!columnFamily.isMarkedForDelete()) ? columnFamily : null;
  }

  private void getColumnFamilyFromCurrentMemtable(String key, String cf,
      IFilter filter, List<ColumnFamily> columnFamilies) {
    /* Get the ColumnFamily from Memtable */
    ColumnFamily columnFamily = memtable_.get().get(key, cf, filter);
    if (columnFamily != null) {
      if (!columnFamily.isMarkedForDelete())
        columnFamilies.add(columnFamily);
    }
  }

  private ColumnFamily resolve(List<ColumnFamily> columnFamilies) {
    int size = columnFamilies.size();
    if (size == 0)
      return null;
    // ColumnFamily cf = new ColumnFamily(columnFamily_);
    ColumnFamily cf = columnFamilies.get(0);
    for (int i = 1; i < size; ++i) {
      cf.addColumns(columnFamilies.get(i));
    }
    return cf;
  }

  /*
   * This version is used only on start up when we are recovering from logs.
   * Hence no locking is required since we process logs on the main thread. In
   * the future we may want to parellelize the log processing for a table by
   * having a thread per log file present for recovery. Re-visit at that time.
   */
  void applyNow(String key, ColumnFamily columnFamily) throws IOException {
    if (!columnFamily.isMarkedForDelete())
      memtable_.get().putOnRecovery(key, columnFamily);
  }

  /*
   * Delete doesn't mean we can blindly delete. We need to write this to disk as
   * being marked for delete. This is to prevent a previous value from
   * resuscitating a column family that has been deleted.
   */
  void delete(String key, ColumnFamily columnFamily) throws IOException {
    memtable_.get().remove(key, columnFamily);
  }

  /*
   * This method is called when the Memtable is frozen and ready to be flushed
   * to disk. This method informs the CommitLog that a particular ColumnFamily
   * is being flushed to disk.
   */
  void onMemtableFlush(CommitLog.CommitLogContext cLogCtx) throws IOException {
    if (cLogCtx.isValidContext())
      CommitLog.open(table_).onMemtableFlush(columnFamily_, cLogCtx);
  }

  /*
   * Called after the Memtable flushes its in-memory data. This information is
   * cached in the ColumnFamilyStore. This is useful for reads because the
   * ColumnFamilyStore first looks in the in-memory store and the into the disk
   * to find the key. If invoked during recoveryMode the onMemtableFlush() need
   * not be invoked.
   * 
   * param @ filename - filename just flushed to disk param @ bf - bloom filter
   * which indicates the keys that are in this file.
   */
  void storeLocation(String filename, BloomFilter bf) throws IOException {
    boolean doCompaction = false;
    int ssTableSize = 0;
    lock_.writeLock().lock();
    try {
      ssTables_.add(filename);
      SSTable.storeBloomFilter(filename, bf);
      ssTableSize = ssTables_.size();
    } finally {
      lock_.writeLock().unlock();
    }
    if (ssTableSize >= threshHold_ && !isCompacting_.get()) {
      doCompaction = true;
    }

    if (isCompacting_.get()) {
      if (ssTableSize % threshHold_ == 0) {
        doCompaction = true;
      }
    }
    if (doCompaction) {
      logger_.debug("Submitting for  compaction ...");
      MinorCompactionManager.instance().submit(ColumnFamilyStore.this);
      logger_.debug("Submitted for compaction ...");
    }
  }

  PriorityQueue<FileStruct> initializePriorityQueue(List<String> files,
      List<Range> ranges, int minBufferSize) throws IOException {
    PriorityQueue<FileStruct> pq = new PriorityQueue<FileStruct>();
    if (files.size() > 1 || (ranges != null && files.size() > 0)) {
      int bufferSize = Math.min(
          (ColumnFamilyStore.compactionMemoryThreshold_ / files.size()),
          minBufferSize);
      FileStruct fs = null;
      for (String file : files) {
        try {
          fs = new FileStruct();
          fs.bufIn = new DataInputBuffer();
          fs.bufOut = new DataOutputBuffer();
          fs.reader = SequenceFile.bufferedReader(file, bufferSize);
          fs.key = null;
          fs = getNextKey(fs);
          if (fs == null)
            continue;
          pq.add(fs);
        } catch (Exception ex) {
          ex.printStackTrace();
          try {
            if (fs != null) {
              fs.reader.close();
            }
          } catch (Exception e) {
            logger_.warn("Unable to close file :" + file);
          }
          continue;
        }
      }
    }
    return pq;
  }

  /*
   * Stage the compactions , compact similar size files. This fn figures out the
   * files close enough by size and if they are greater than the threshold then
   * compacts.
   */
  Map<Integer, List<String>> stageCompaction(List<String> files) {
    Map<Integer, List<String>> buckets = new HashMap<Integer, List<String>>();
    long averages[] = new long[100];
    int count = 0;
    long max = 200L * 1024L * 1024L * 1024L;
    long min = 50L * 1024L * 1024L;
    List<String> largeFileList = new ArrayList<String>();
    for (String file : files) {
      File f = new File(file);
      long size = f.length();
      if (size > max) {
        largeFileList.add(file);
        continue;
      }
      boolean bFound = false;
      for (int i = 0; i < count; i++) {
        if ((size > averages[i] / 2 && size < 3 * averages[i] / 2)
            || (size < min && averages[i] < min)) {
          averages[i] = (averages[i] + size) / 2;
          List<String> fileList = buckets.get(i);
          if (fileList == null) {
            fileList = new ArrayList<String>();
            buckets.put(i, fileList);
          }
          fileList.add(file);
          bFound = true;
          break;
        }
      }
      if (!bFound) {
        List<String> fileList = buckets.get(count);
        if (fileList == null) {
          fileList = new ArrayList<String>();
          buckets.put(count, fileList);
        }
        fileList.add(file);
        averages[count] = size;
        count++;
      }

    }
    // Put files greater than teh max in a separate bucket so that they are
    // never compacted
    // but we need them in the buckets since for range compactions we need to
    // split these files.
    count++;
    for (String file : largeFileList) {
      List<String> tempLargeFileList = new ArrayList<String>();
      tempLargeFileList.add(file);
      buckets.put(count, tempLargeFileList);
      count++;
    }
    return buckets;
  }

  /*
   * Stage the compactions , compact similar size files. This fn figures out the
   * files close enough by size and if they are greater than the threshold then
   * compacts.
   */
  Map<Integer, List<String>> stageOrderedCompaction(List<String> files) {
    Map<Integer, List<String>> buckets = new HashMap<Integer, List<String>>();
    long averages[] = new long[100];
    int count = 0;
    long max = 200L * 1024L * 1024L * 1024L;
    long min = 50L * 1024L * 1024L;
    List<String> largeFileList = new ArrayList<String>();
    for (String file : files) {
      File f = new File(file);
      long size = f.length();
      if (size > max) {
        largeFileList.add(file);
        continue;
      }
      boolean bFound = false;
      for (int i = 0; i < count; i++) {
        if ((size > averages[i] / 2 && size < 3 * averages[i] / 2)
            || (size < min && averages[i] < min)) {
          averages[i] = (averages[i] + size) / 2;
          List<String> fileList = buckets.get(i);
          if (fileList == null) {
            fileList = new ArrayList<String>();
            buckets.put(i, fileList);
          }
          fileList.add(file);
          bFound = true;
          break;
        }
      }
      if (!bFound) {
        List<String> fileList = buckets.get(count);
        if (fileList == null) {
          fileList = new ArrayList<String>();
          buckets.put(count, fileList);
        }
        fileList.add(file);
        averages[count] = size;
        count++;
      }

    }
    // Put files greater than teh max in a separate bucket so that they are
    // never compacted
    // but we need them in the buckets since for range compactions we need to
    // split these files.
    count++;
    for (String file : largeFileList) {
      List<String> tempLargeFileList = new ArrayList<String>();
      tempLargeFileList.add(file);
      buckets.put(count, tempLargeFileList);
      count++;
    }
    return buckets;
  }

  /*
   * Break the files into buckets and then compact.
   */
  BloomFilter.CountingBloomFilter doCompaction(List<Range> ranges)
      throws IOException {
    isCompacting_.set(true);
    List<String> files = new ArrayList<String>(ssTables_);
    BloomFilter.CountingBloomFilter result = null;
    try {
      int count = 0;
      Map<Integer, List<String>> buckets = stageCompaction(files);
      Set<Integer> keySet = buckets.keySet();
      for (Integer key : keySet) {
        List<String> fileList = buckets.get(key);
        BloomFilter.CountingBloomFilter tempResult = null;
        // If ranges != null we should split the files irrespective of the
        // threshold.
        if (fileList.size() >= threshHold_ || ranges != null) {
          files.clear();
          count = 0;
          for (String file : fileList) {
            files.add(file);
            count++;
            if (count == threshHold_ && ranges == null)
              break;
          }
          try {
            // For each bucket if it has crossed the threshhold do the
            // compaction
            // In case of range compaction merge the counting bloom filters
            // also.
            if (ranges == null)
              tempResult = doRangeCompaction(files, ranges, bufSize_);
            else
              tempResult = doRangeCompaction(files, ranges, bufSize_);

            if (result == null) {
              result = tempResult;
            } else {
              result.merge(tempResult);
            }
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        }
      }
    } finally {
      isCompacting_.set(false);
    }
    return result;
  }

  BloomFilter.CountingBloomFilter doMajorCompaction(long skip)
      throws IOException {
    return doMajorCompactionInternal(skip);
  }

  BloomFilter.CountingBloomFilter doMajorCompaction() throws IOException {
    return doMajorCompactionInternal(0);
  }

  /*
   * Compact all the files irrespective of the size. skip : is the ammount in Gb
   * of the files to be skipped all files greater than skip GB are skipped for
   * this compaction. Except if skip is 0 , in that case this is ignored and all
   * files are taken.
   */
  BloomFilter.CountingBloomFilter doMajorCompactionInternal(long skip)
      throws IOException {
    isCompacting_.set(true);
    List<String> filesInternal = new ArrayList<String>(ssTables_);
    List<String> files = null;
    BloomFilter.CountingBloomFilter result = null;
    try {
      if (skip > 0L) {
        files = new ArrayList<String>();
        for (String file : filesInternal) {
          File f = new File(file);
          if (f.length() < skip * 1024L * 1024L * 1024L) {
            files.add(file);
          }
        }
      } else {
        files = filesInternal;
      }
      result = doRangeCompaction(files, null, bufSize_);
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      isCompacting_.set(false);
    }
    return result;
  }

  /*
   * Add up all the files sizes this is the worst case file size for compaction
   * of all the list of files given.
   */
  long getExpectedCompactedFileSize(List<String> files) {
    long expectedFileSize = 0;
    for (String file : files) {
      File f = new File(file);
      long size = f.length();
      expectedFileSize = expectedFileSize + size;
    }
    return expectedFileSize;
  }

  /*
   * Find the maximum size file in the list .
   */
  String getMaxSizeFile(List<String> files) {
    long maxSize = 0L;
    String maxFile = null;
    for (String file : files) {
      File f = new File(file);
      if (f.length() > maxSize) {
        maxSize = f.length();
        maxFile = file;
      }
    }
    return maxFile;
  }

  Range getMaxRange(List<Range> ranges) {
    Range maxRange = new Range(BigInteger.ZERO, BigInteger.ZERO);
    for (Range range : ranges) {
      if (range.left().compareTo(maxRange.left()) > 0) {
        maxRange = range;
      }
    }
    return maxRange;
  }

  boolean isLoopAround(List<Range> ranges) {
    boolean isLoop = false;
    for (Range range : ranges) {
      if (range.left().compareTo(range.right()) > 0) {
        isLoop = true;
        break;
      }
    }
    return isLoop;
  }

  BloomFilter.CountingBloomFilter doRangeAntiCompaction(List<Range> ranges,
      EndPoint target, List<String> fileList) throws IOException {
    isCompacting_.set(true);
    List<String> files = new ArrayList<String>(ssTables_);
    BloomFilter.CountingBloomFilter result = null;
    try {
      result = doRangeOnlyAntiCompaction(files, ranges, target, bufSize_,
          fileList, null);
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      isCompacting_.set(false);
    }
    return result;

  }

  /*
   * Read the next key from the data file , this fn will skip teh block index
   * and read teh next available key into the filestruct that is passed. If it
   * cannot read or a end of file is reached it will return null.
   */
  FileStruct getNextKey(FileStruct filestruct) throws IOException {
    filestruct.bufOut.reset();
    if (filestruct.reader.isEOF()) {
      filestruct.reader.close();
      return null;
    }

    long bytesread = filestruct.reader.next(filestruct.bufOut);
    if (bytesread == -1) {
      filestruct.reader.close();
      return null;
    }

    filestruct.bufIn.reset(filestruct.bufOut.getData(), filestruct.bufOut
        .getLength());
    filestruct.key = filestruct.bufIn.readUTF();
    /*
     * If the key we read is the Block Index Key then omit and read the next
     * key.
     */
    if (filestruct.key.equals(SSTable.blockIndexKey_)) {
      filestruct.bufOut.reset();
      bytesread = filestruct.reader.next(filestruct.bufOut);
      if (bytesread == -1) {
        filestruct.reader.close();
        return null;
      }
      filestruct.bufIn.reset(filestruct.bufOut.getData(), filestruct.bufOut
          .getLength());
      filestruct.key = filestruct.bufIn.readUTF();
    }
    return filestruct;
  }

  void forceCleanup() {
    MinorCompactionManager.instance().submitCleanup(ColumnFamilyStore.this);
  }

  /**
   * This function goes over each file and removes the keys that the node is not
   * responsible for and only keeps keys that this node is responsible for.
   * 
   * @throws IOException
   */
  void doCleanupCompaction() throws IOException {
    isCompacting_.set(true);
    List<String> files = new ArrayList<String>(ssTables_);
    for (String file : files) {
      try {
        doCleanup(file);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    isCompacting_.set(false);
  }

  /**
   * cleans up one particular file by removing keys that this node is not
   * responsible for.
   * 
   * @param file
   * @throws IOException
   */
  /* TODO: Take care of the comments later. */
  void doCleanup(String file) throws IOException {
    if (file == null)
      return;
    List<Range> myRanges = null;
    List<String> files = new ArrayList<String>();
    files.add(file);
    List<String> newFiles = new ArrayList<String>();
    Map<EndPoint, List<Range>> endPointtoRangeMap = StorageService.instance()
        .constructEndPointToRangesMap();
    myRanges = endPointtoRangeMap.get(StorageService.getLocalStorageEndPoint());
    List<BloomFilter> compactedBloomFilters = new ArrayList<BloomFilter>();
    doRangeOnlyAntiCompaction(files, myRanges, null, bufSize_, newFiles,
        compactedBloomFilters);
    logger_.info("Original file : " + file + " of size "
        + new File(file).length());
    lock_.writeLock().lock();
    try {
      ssTables_.remove(file);
      SSTable.removeAssociatedBloomFilter(file);
      for (String newfile : newFiles) {
        logger_.info("New file : " + newfile + " of size "
            + new File(newfile).length());
        if (newfile != null) {
          ssTables_.add(newfile);
          logger_.info("Inserting bloom filter for file " + newfile);
          SSTable.storeBloomFilter(newfile, compactedBloomFilters.get(0));
        }
      }
      SSTable.delete(file);
    } finally {
      lock_.writeLock().unlock();
    }
  }

  /**
   * This function is used to do the anti compaction process , it spits out the
   * file which has keys that belong to a given range If the target is not
   * specified it spits out the file as a compacted file with the unecessary
   * ranges wiped out.
   * 
   * @param files
   * @param ranges
   * @param target
   * @param minBufferSize
   * @param fileList
   * @return
   * @throws IOException
   */
  BloomFilter.CountingBloomFilter doRangeOnlyAntiCompaction(List<String> files,
      List<Range> ranges, EndPoint target, int minBufferSize,
      List<String> fileList, List<BloomFilter> compactedBloomFilters)
      throws IOException {
    BloomFilter.CountingBloomFilter rangeCountingBloomFilter = null;
    long startTime = System.currentTimeMillis();
    long totalBytesRead = 0;
    long totalBytesWritten = 0;
    long totalkeysRead = 0;
    long totalkeysWritten = 0;
    String rangeFileLocation = null;
    String mergedFileName = null;
    try {
      // Calculate the expected compacted filesize
      long expectedRangeFileSize = getExpectedCompactedFileSize(files);
      /*
       * in the worst case a node will be giving out alf of its data so we take
       * a chance
       */
      expectedRangeFileSize = expectedRangeFileSize / 2;
      rangeFileLocation = DatabaseDescriptor
          .getCompactionFileLocation(expectedRangeFileSize);
      // boolean isLoop = isLoopAround( ranges );
      // Range maxRange = getMaxRange( ranges );
      // If the compaction file path is null that means we have no space left
      // for this compaction.
      if (rangeFileLocation == null) {
        logger_.warn("Total bytes to be written for range compaction  ..."
            + expectedRangeFileSize
            + "   is greater than the safe limit of the disk space available.");
        return null;
      }
      PriorityQueue<FileStruct> pq = initializePriorityQueue(files, ranges,
          minBufferSize);
      if (pq.size() > 0) {
        mergedFileName = getTempFileName();
        SSTable ssTableRange = null;
        String lastkey = null;
        List<FileStruct> lfs = new ArrayList<FileStruct>();
        DataOutputBuffer bufOut = new DataOutputBuffer();
        int expectedBloomFilterSize = SSTable.getApproximateKeyCount(files);
        expectedBloomFilterSize = (expectedBloomFilterSize > 0) ? expectedBloomFilterSize
            : SSTable.indexInterval();
        logger_
            .debug("Expected bloom filter size : " + expectedBloomFilterSize);
        /* Create the bloom filter for the compacted file. */
        BloomFilter compactedRangeBloomFilter = new BloomFilter(
            expectedBloomFilterSize, 8);
        List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();

        while (pq.size() > 0 || lfs.size() > 0) {
          FileStruct fs = null;
          if (pq.size() > 0) {
            fs = pq.poll();
          }
          if (fs != null && (lastkey == null || lastkey.compareTo(fs.key) == 0)) {
            // The keys are the same so we need to add this to the
            // ldfs list
            lastkey = fs.key;
            lfs.add(fs);
          } else {
            Collections.sort(lfs, new FileStructComparator());
            ColumnFamily columnFamily = null;
            bufOut.reset();
            if (lfs.size() > 1) {
              for (FileStruct filestruct : lfs) {
                try {
                  /* read the length although we don't need it */
                  filestruct.bufIn.readInt();
                  // Skip the Index
                  if (DatabaseDescriptor.isNameIndexEnabled(columnFamily_)) {
                    IndexHelper.skip(filestruct.bufIn);
                  }
                  // We want to add only 2 and resolve them right there in order
                  // to save on memory footprint
                  if (columnFamilies.size() > 1) {
                    // Now merge the 2 column families
                    columnFamily = resolve(columnFamilies);
                    columnFamilies.clear();
                    if (columnFamily != null) {
                      // add the merged columnfamily back to the list
                      columnFamilies.add(columnFamily);
                    }

                  }
                  // deserialize into column families
                  columnFamilies.add(ColumnFamily.serializer().deserialize(
                      filestruct.bufIn));
                } catch (Exception ex) {
                  logger_.warn(LogUtil.throwableToString(ex));
                  continue;
                }
              }
              // Now after merging all crap append to the sstable
              columnFamily = resolve(columnFamilies);
              columnFamilies.clear();
              if (columnFamily != null) {
                /* serialize the cf with column indexes */
                ColumnFamily.serializer2().serialize(columnFamily, bufOut);
              }
            } else {
              FileStruct filestruct = lfs.get(0);
              try {
                /* read the length although we don't need it */
                int size = filestruct.bufIn.readInt();
                bufOut.write(filestruct.bufIn, size);
              } catch (Exception ex) {
                logger_.warn(LogUtil.throwableToString(ex));
                filestruct.reader.close();
                continue;
              }
            }
            if (Range.isKeyInRanges(ranges, lastkey)) {
              if (ssTableRange == null) {
                if (target != null)
                  rangeFileLocation = rangeFileLocation
                      + System.getProperty("file.separator") + "bootstrap";
                FileUtils.createDirectory(rangeFileLocation);
                ssTableRange = new SSTable(rangeFileLocation, mergedFileName);
              }
              if (rangeCountingBloomFilter == null) {
                rangeCountingBloomFilter = new BloomFilter.CountingBloomFilter(
                    expectedBloomFilterSize, 8);
              }
              try {
                ssTableRange.append(lastkey, bufOut);
                compactedRangeBloomFilter.fill(lastkey);
                if (target != null
                    && StorageService.instance().isPrimary(lastkey, target)) {
                  rangeCountingBloomFilter.add(lastkey);
                  StorageService.instance().delete(lastkey);
                }
              } catch (Exception ex) {
                logger_.warn(LogUtil.throwableToString(ex));
              }
            }
            totalkeysWritten++;
            for (FileStruct filestruct : lfs) {
              try {
                filestruct = getNextKey(filestruct);
                if (filestruct == null) {
                  continue;
                }
                /* keep on looping until we find a key in the range */
                while (!Range.isKeyInRanges(ranges, filestruct.key)) {
                  filestruct = getNextKey(filestruct);
                  if (filestruct == null) {
                    break;
                  }
                  /*
                   * check if we need to continue , if we are done with ranges
                   * empty the queue and close all file handles and exit
                   */
                  // if( !isLoop &&
                  // StorageService.hash(filestruct.key).compareTo(maxRange.right())
                  // > 0 && !filestruct.key.equals(""))
                  // {
                  // filestruct.reader.close();
                  // filestruct = null;
                  // break;
                  // }
                }
                if (filestruct != null) {
                  pq.add(filestruct);
                }
                totalkeysRead++;
              } catch (Exception ex) {
                // Ignore the exception as it might be a corrupted file
                // in any case we have read as far as possible from it
                // and it will be deleted after compaction.
                logger_.warn(LogUtil.throwableToString(ex));
                filestruct.reader.close();
                continue;
              }
            }
            lfs.clear();
            lastkey = null;
            if (fs != null) {
              // Add back the fs since we processed the rest of
              // filestructs
              pq.add(fs);
            }
          }
        }
        if (ssTableRange != null) {
          if (fileList == null)
            fileList = new ArrayList<String>();
          ssTableRange.closeRename(compactedRangeBloomFilter, fileList);
          if (compactedBloomFilters != null)
            compactedBloomFilters.add(compactedRangeBloomFilter);
        }
      }
    } catch (Exception ex) {
      logger_.warn(LogUtil.throwableToString(ex));
    }
    logger_.debug("Total time taken for range split   ..."
        + (System.currentTimeMillis() - startTime));
    logger_.debug("Total bytes Read for range split  ..." + totalBytesRead);
    logger_.debug("Total bytes written for range split  ..."
        + totalBytesWritten + "   Total keys read ..." + totalkeysRead);
    return rangeCountingBloomFilter;
  }

  /*
   * This function does the actual compaction for files. It maintains a priority
   * queue of with the first key from each file and then removes the top of the
   * queue and adds it to the SStable and repeats this process while reading the
   * next from each file until its done with all the files . The SStable to
   * which the keys are written represents the new compacted file. Before
   * writing if there are keys that occur in multiple files and are the same
   * then a resolution is done to get the latest data.
   */
  BloomFilter.CountingBloomFilter doRangeCompaction(List<String> files,
      List<Range> ranges, int minBufferSize) throws IOException {
    BloomFilter.CountingBloomFilter rangeCountingBloomFilter = null;
    String newfile = null;
    long startTime = System.currentTimeMillis();
    long totalBytesRead = 0;
    long totalBytesWritten = 0;
    long totalkeysRead = 0;
    long totalkeysWritten = 0;
    try {
      // Calculate the expected compacted filesize
      long expectedCompactedFileSize = getExpectedCompactedFileSize(files);
      String compactionFileLocation = DatabaseDescriptor
          .getCompactionFileLocation(expectedCompactedFileSize);
      // If the compaction file path is null that means we have no space left
      // for this compaction.
      if (compactionFileLocation == null) {
        if (ranges == null || ranges.size() == 0) {
          String maxFile = getMaxSizeFile(files);
          files.remove(maxFile);
          return doRangeCompaction(files, ranges, minBufferSize);
        }
        logger_.warn("Total bytes to be written for compaction  ..."
            + expectedCompactedFileSize
            + "   is greater than the safe limit of the disk space available.");
        return null;
      }
      PriorityQueue<FileStruct> pq = initializePriorityQueue(files, ranges,
          minBufferSize);
      if (pq.size() > 0) {
        String mergedFileName = getTempFileName();
        SSTable ssTable = null;
        SSTable ssTableRange = null;
        String lastkey = null;
        List<FileStruct> lfs = new ArrayList<FileStruct>();
        DataOutputBuffer bufOut = new DataOutputBuffer();
        int expectedBloomFilterSize = SSTable.getApproximateKeyCount(files);
        expectedBloomFilterSize = (expectedBloomFilterSize > 0) ? expectedBloomFilterSize
            : SSTable.indexInterval();
        logger_
            .debug("Expected bloom filter size : " + expectedBloomFilterSize);
        /* Create the bloom filter for the compacted file. */
        BloomFilter compactedBloomFilter = new BloomFilter(
            expectedBloomFilterSize, 8);
        BloomFilter compactedRangeBloomFilter = new BloomFilter(
            expectedBloomFilterSize, 8);
        List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();

        while (pq.size() > 0 || lfs.size() > 0) {
          FileStruct fs = null;
          if (pq.size() > 0) {
            fs = pq.poll();
          }
          if (fs != null && (lastkey == null || lastkey.compareTo(fs.key) == 0)) {
            // The keys are the same so we need to add this to the
            // ldfs list
            lastkey = fs.key;
            lfs.add(fs);
          } else {
            Collections.sort(lfs, new FileStructComparator());
            ColumnFamily columnFamily = null;
            bufOut.reset();
            if (lfs.size() > 1) {
              for (FileStruct filestruct : lfs) {
                try {
                  /* read the length although we don't need it */
                  filestruct.bufIn.readInt();
                  // Skip the Index
                  if (DatabaseDescriptor.isNameIndexEnabled(columnFamily_)) {
                    IndexHelper.skip(filestruct.bufIn);
                  }
                  // We want to add only 2 and resolve them right there in order
                  // to save on memory footprint
                  if (columnFamilies.size() > 1) {
                    // Now merge the 2 column families
                    columnFamily = resolve(columnFamilies);
                    columnFamilies.clear();
                    if (columnFamily != null) {
                      // add the merged columnfamily back to the list
                      columnFamilies.add(columnFamily);
                    }

                  }
                  // deserialize into column families
                  columnFamilies.add(ColumnFamily.serializer().deserialize(
                      filestruct.bufIn));
                } catch (Exception ex) {
                  ex.printStackTrace();
                  continue;
                }
              }
              // Now after merging all crap append to the sstable
              columnFamily = resolve(columnFamilies);
              columnFamilies.clear();
              if (columnFamily != null) {
                /* serialize the cf with column indexes */
                ColumnFamily.serializer2().serialize(columnFamily, bufOut);
              }
            } else {
              FileStruct filestruct = lfs.get(0);
              try {
                /* read the length although we don't need it */
                int size = filestruct.bufIn.readInt();
                bufOut.write(filestruct.bufIn, size);
              } catch (Exception ex) {
                ex.printStackTrace();
                filestruct.reader.close();
                continue;
              }
            }
            if (Range.isKeyInRanges(ranges, lastkey)) {
              if (ssTableRange == null) {
                String mergedRangeFileName = getTempFileName();
                ssTableRange = new SSTable(DatabaseDescriptor
                    .getBootstrapFileLocation(), mergedRangeFileName);
              }
              if (rangeCountingBloomFilter == null) {
                rangeCountingBloomFilter = new BloomFilter.CountingBloomFilter(
                    expectedBloomFilterSize, 8);
              }
              ssTableRange.append(lastkey, bufOut);
              compactedRangeBloomFilter.fill(lastkey);
              rangeCountingBloomFilter.add(lastkey);
            } else {
              if (ssTable == null) {
                ssTable = new SSTable(compactionFileLocation, mergedFileName);
              }
              try {
                ssTable.append(lastkey, bufOut);
              } catch (Exception ex) {
                logger_.warn(LogUtil.throwableToString(ex));
              }

              /* Fill the bloom filter with the key */
              compactedBloomFilter.fill(lastkey);
            }
            totalkeysWritten++;
            for (FileStruct filestruct : lfs) {
              try {
                filestruct = getNextKey(filestruct);
                if (filestruct == null) {
                  continue;
                }
                pq.add(filestruct);
                totalkeysRead++;
              } catch (Exception ex) {
                // Ignore the exception as it might be a corrupted file
                // in any case we have read as far as possible from it
                // and it will be deleted after compaction.
                ex.printStackTrace();
                filestruct.reader.close();
                continue;
              }
            }
            lfs.clear();
            lastkey = null;
            if (fs != null) {
              // Add back the fs since we processed the rest of
              // filestructs
              pq.add(fs);
            }
          }
        }
        if (ssTable != null) {
          ssTable.closeRename(compactedBloomFilter);
          newfile = ssTable.getDataFileLocation();
        }
        if (ssTableRange != null) {
          ssTableRange.closeRename(compactedRangeBloomFilter);
        }
        lock_.writeLock().lock();
        try {
          for (String file : files) {
            ssTables_.remove(file);
            SSTable.removeAssociatedBloomFilter(file);
          }
          if (newfile != null) {
            ssTables_.add(newfile);
            logger_.info("Inserting bloom filter for file " + newfile);
            SSTable.storeBloomFilter(newfile, compactedBloomFilter);
            totalBytesWritten = (new File(newfile)).length();
          }
        } finally {
          lock_.writeLock().unlock();
        }
        for (String file : files) {
          SSTable.delete(file);
        }
      }
    } catch (Exception ex) {
      logger_.warn(LogUtil.throwableToString(ex));
    }
    logger_.debug("Total time taken for compaction  ..."
        + (System.currentTimeMillis() - startTime));
    logger_.debug("Total bytes Read for compaction  ..." + totalBytesRead);
    logger_.debug("Total bytes written for compaction  ..." + totalBytesWritten
        + "   Total keys read ..." + totalkeysRead);
    return rangeCountingBloomFilter;
  }

  public static void main(String[] args) throws Throwable {
    LogUtil.init();
    StorageService s = StorageService.instance();
    s.start();
    /*
     * Table table = Table.open("Mailbox"); Random random = new Random(); byte[]
     * bytes = new byte[1024];
     * 
     * for (int i = 100; i < 1000; ++i) { String key = Integer.toString(i);
     * RowMutation rm = new RowMutation("Mailbox", key); for ( int j = 0; j <
     * 16; ++j ) { for ( int k = 0; k < 16; ++k ) { random.nextBytes(bytes);
     * rm.add("MailboxUserList0:" + "SuperColumn-" + j + ":Column-" + k, bytes,
     * k); rm.add("MailboxMailList0:" + "Column-" + k, bytes, k); } }
     * rm.apply(); } System.out.println("Write done");
     */

    Table table = Table.open("Mailbox");
    while (true) {
      for (int i = 100; i < 1000; ++i) {
        String key = Integer.toString(i);
        ColumnFamily cf = table.get(key, "MailboxUserList0:SuperColumn-1");
        if (cf == null)
          System.out.println("KEY " + key + " is missing");
        else {
          Collection<IColumn> superColumns = cf.getAllColumns();
          for (IColumn superColumn : superColumns) {
            Collection<IColumn> subColumns = superColumn.getSubColumns();
            for (IColumn subColumn : subColumns) {
              // System.out.println(subColumn);
            }
          }
          // System.out.println("Success ...");
        }
      }
      System.out.println("Read done ...");
    }
  }
}
