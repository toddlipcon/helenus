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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.math.linear.RealMatrix;
import org.apache.commons.math.linear.RealMatrixImpl;
import org.apache.log4j.Logger;

import com.facebook.infrastructure.analytics.AnalyticsContext;
import com.facebook.infrastructure.concurrent.DebuggableThreadPoolExecutor;
import com.facebook.infrastructure.concurrent.MultiThreadedStage;
import com.facebook.infrastructure.concurrent.SingleThreadedStage;
import com.facebook.infrastructure.concurrent.StageManager;
import com.facebook.infrastructure.concurrent.ThreadFactoryImpl;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.db.BinaryVerbHandler;
import com.facebook.infrastructure.db.DBManager;
import com.facebook.infrastructure.db.FileUtils;
import com.facebook.infrastructure.db.HintedHandOffManager;
import com.facebook.infrastructure.db.LoadVerbHandler;
import com.facebook.infrastructure.db.Memtable;
import com.facebook.infrastructure.db.ReadRepairVerbHandler;
import com.facebook.infrastructure.db.ReadVerbHandler;
import com.facebook.infrastructure.db.Row;
import com.facebook.infrastructure.db.RowMutationVerbHandler;
import com.facebook.infrastructure.db.SystemTable;
import com.facebook.infrastructure.db.Table;
import com.facebook.infrastructure.dht.BootStrapper;
import com.facebook.infrastructure.dht.BootstrapInitiateMessage;
import com.facebook.infrastructure.dht.BootstrapMetadataVerbHandler;
import com.facebook.infrastructure.dht.Range;
import com.facebook.infrastructure.gms.ApplicationState;
import com.facebook.infrastructure.gms.EndPointState;
import com.facebook.infrastructure.gms.FailureDetector;
import com.facebook.infrastructure.gms.Gossiper;
import com.facebook.infrastructure.gms.IEndPointStateChangeSubscriber;
import com.facebook.infrastructure.locator.EndPointSnitch;
import com.facebook.infrastructure.locator.IEndPointSnitch;
import com.facebook.infrastructure.locator.IReplicaPlacementStrategy;
import com.facebook.infrastructure.locator.RackAwareStrategy;
import com.facebook.infrastructure.locator.RackUnawareStrategy;
import com.facebook.infrastructure.locator.TokenMetadata;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.net.IVerbHandler;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.MessagingService;
import com.facebook.infrastructure.net.http.HttpConnection;
import com.facebook.infrastructure.net.io.StreamContextManager;
import com.facebook.infrastructure.tools.MembershipCleanerVerbHandler;
import com.facebook.infrastructure.tools.TokenUpdateVerbHandler;
import com.facebook.infrastructure.utils.LogUtil;
import com.yahoo.zookeeper.KeeperException;
import com.yahoo.zookeeper.Watcher;
import com.yahoo.zookeeper.ZooKeeper;
import com.yahoo.zookeeper.ZooDefs.Ids;
import com.yahoo.zookeeper.data.Stat;
import com.yahoo.zookeeper.proto.WatcherEvent;

/*
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public final class StorageService implements IEndPointStateChangeSubscriber,
    StorageServiceMBean {
  private static Logger logger_ = Logger.getLogger(StorageService.class);
  private static final BigInteger prime_ = BigInteger.valueOf(31);
  private final static int maxKeyHashLength_ = 24;
  private final static String nodeId_ = "NODE-IDENTIFIER";
  private final static String loadAll_ = "LOAD-ALL";
  public final static String mutationStage_ = "ROW-MUTATION-STAGE";
  public final static String readStage_ = "ROW-READ-STAGE";
  public final static String mutationVerbHandler_ = "ROW-MUTATION-VERB-HANDLER";
  public final static String tokenVerbHandler_ = "TOKEN-VERB-HANDLER";
  public final static String loadVerbHandler_ = "LOAD-VERB-HANDLER";
  public final static String binaryVerbHandler_ = "BINARY-VERB-HANDLER";
  public final static String readRepairVerbHandler_ = "READ-REPAIR-VERB-HANDLER";
  public final static String readVerbHandler_ = "ROW-READ-VERB-HANDLER";
  public final static String bootStrapInitiateVerbHandler_ = "BOOTSTRAP-INITIATE-VERB-HANDLER";
  public final static String bootStrapInitiateDoneVerbHandler_ = "BOOTSTRAP-INITIATE-DONE-VERB-HANDLER";
  public final static String bootStrapTerminateVerbHandler_ = "BOOTSTRAP-TERMINATE-VERB-HANDLER";
  public final static String tokenInfoVerbHandler_ = "TOKENINFO-VERB-HANDLER";
  public final static String mbrshipCleanerVerbHandler_ = "MBRSHIP-CLEANER-VERB-HANDLER";
  public final static String bsMetadataVerbHandler_ = "BS-METADATA-VERB-HANDLER";

  public static enum ConsistencyLevel {
    WEAK, STRONG
  };

  private static StorageService instance_;
  /* Used to lock the factory for creation of StorageService instance */
  private static Lock createLock_ = new ReentrantLock();
  private static EndPoint tcpAddr_;
  private static EndPoint udpAddr_;

  public static EndPoint getLocalStorageEndPoint() {
    return tcpAddr_;
  }

  public static EndPoint getLocalControlEndPoint() {
    return udpAddr_;
  }

  public static String getHostUrl() {
    return "http://" + tcpAddr_.getHost() + ":"
        + DatabaseDescriptor.getHttpPort();
  }

  /*
   * Order preserving hash for the specified key.
   */
  public static BigInteger hash(String key) {
    BigInteger h = BigInteger.ZERO;
    char val[] = key.toCharArray();
    for (int i = 0; i < StorageService.maxKeyHashLength_; i++) {
      if (i < val.length)
        h = StorageService.prime_.multiply(h).add(BigInteger.valueOf(val[i]));
      else
        h = StorageService.prime_.multiply(h).add(StorageService.prime_);
    }

    return h;
  }

  public static enum BootstrapMode {
    HINT, FULL
  };

  public static class BootstrapInitiateDoneVerbHandler implements IVerbHandler {
    private static Logger logger_ = Logger
        .getLogger(BootstrapInitiateDoneVerbHandler.class);

    public void doVerb(Message message) {
      logger_.debug("Received a bootstrap initiate done message ...");
      /* Let the Stream Manager do his thing. */
      StreamManager.instance(message.getFrom()).start();
    }
  }

  private class ShutdownTimerTask extends TimerTask {
    public void run() {
      StorageService.instance().shutdown();
    }
  }

  /*
   * Factory method that gets an instance of the StorageService class.
   */
  public static StorageService instance() {
    if (instance_ == null) {
      StorageService.createLock_.lock();
      try {
        if (instance_ == null) {
          try {
            instance_ = new StorageService();
          } catch (Throwable th) {
            logger_.error(LogUtil.throwableToString(th));
            System.exit(1);
          }
        }
      } finally {
        createLock_.unlock();
      }
    }
    return instance_;
  }

  /*
   * This is the endpoint snitch which depends on the network architecture. We
   * need to keep this information for each endpoint so that we make decisions
   * while doing things like replication etc.
   */
  private IEndPointSnitch endPointSnitch_;
  /*
   * Uptime of this node - we use this to determine if a bootstrap can be
   * performed by this node
   */
  private long uptime_ = 0L;

  /* This abstraction maintains the token/endpoint metadata information */
  private TokenMetadata tokenMetadata_ = new TokenMetadata();
  private DBManager.StorageMetadata storageMetadata_;

  /*
   * Maintains a list of all components that need to be shutdown for a clean
   * exit.
   */
  private Set<IComponentShutdown> components_ = new HashSet<IComponentShutdown>();
  /*
   * This boolean indicates if we are in loading state. If we are then we do not
   * want any distributed algorithms w.r.t change in token state to kick in.
   */
  private boolean isLoadState_ = false;

  /*
   * This variable indicates if the local storage instance has been shutdown.
   */
  private AtomicBoolean isShutdown_ = new AtomicBoolean(false);

  /* This thread pool is used to do the bootstrap for a new node */
  private ExecutorService bootStrapper_ = new DebuggableThreadPoolExecutor(1,
      1, Integer.MAX_VALUE, TimeUnit.SECONDS,
      new LinkedBlockingQueue<Runnable>(), new ThreadFactoryImpl(
          "BOOT-STRAPPER"));

  /*
   * This thread pool does consistency checks when the client doesn't care about
   * consistency
   */
  private ExecutorService consistencyManager_;

  /* Helps determine number of keys processed in a time interval */
  private RequestCountSampler sampler_;

  /* This is the entity that tracks load information of all nodes in the cluster */
  private StorageLoadBalancer storageLoadBalancer_;
  /* We use this interface to determine where replicas need to be placed */
  private IReplicaPlacementStrategy nodePicker_;
  /* Handle to a ZooKeeper instance */
  private ZooKeeper zk_;

  /*
   * Registers with Management Server
   */
  private void init() {
    // Register this instance with JMX
    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      mbs.registerMBean(this, new ObjectName(
          "com.facebook.infrastructure.service:type=StorageService"));
    } catch (Exception e) {
      logger_.error(LogUtil.throwableToString(e));
    }
  }

  public StorageService() throws Throwable {
    init();
    uptime_ = System.currentTimeMillis();
    storageLoadBalancer_ = new StorageLoadBalancer(this);
    endPointSnitch_ = new EndPointSnitch();

    /* register the verb handlers */
    MessagingService.getMessagingInstance().registerVerbHandlers(
        StorageService.tokenVerbHandler_, new TokenUpdateVerbHandler());
    MessagingService.getMessagingInstance().registerVerbHandlers(
        StorageService.binaryVerbHandler_, new BinaryVerbHandler());
    MessagingService.getMessagingInstance().registerVerbHandlers(
        StorageService.loadVerbHandler_, new LoadVerbHandler());
    MessagingService.getMessagingInstance().registerVerbHandlers(
        StorageService.mutationVerbHandler_, new RowMutationVerbHandler());
    MessagingService.getMessagingInstance().registerVerbHandlers(
        StorageService.readRepairVerbHandler_, new ReadRepairVerbHandler());
    MessagingService.getMessagingInstance().registerVerbHandlers(
        StorageService.readVerbHandler_, new ReadVerbHandler());
    MessagingService.getMessagingInstance().registerVerbHandlers(
        StorageService.bootStrapInitiateVerbHandler_,
        new Table.BootStrapInitiateVerbHandler());
    MessagingService.getMessagingInstance().registerVerbHandlers(
        StorageService.bootStrapInitiateDoneVerbHandler_,
        new StorageService.BootstrapInitiateDoneVerbHandler());
    MessagingService.getMessagingInstance().registerVerbHandlers(
        StorageService.bootStrapTerminateVerbHandler_,
        new StreamManager.BootstrapTerminateVerbHandler());
    MessagingService.getMessagingInstance().registerVerbHandlers(
        HttpConnection.httpRequestVerbHandler_,
        new HttpRequestVerbHandler(this));
    MessagingService.getMessagingInstance().registerVerbHandlers(
        StorageService.tokenInfoVerbHandler_, new TokenInfoVerbHandler());
    MessagingService.getMessagingInstance().registerVerbHandlers(
        StorageService.mbrshipCleanerVerbHandler_,
        new MembershipCleanerVerbHandler());
    MessagingService.getMessagingInstance().registerVerbHandlers(
        StorageService.bsMetadataVerbHandler_,
        new BootstrapMetadataVerbHandler());

    /* register the stage for the mutations */
    int threadCount = DatabaseDescriptor.getThreadsPerPool();
    consistencyManager_ = new DebuggableThreadPoolExecutor(threadCount,
        threadCount, Integer.MAX_VALUE, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(), new ThreadFactoryImpl(
            "CONSISTENCY-MANAGER"));

    StageManager.registerStage(StorageService.mutationStage_,
        new MultiThreadedStage("ROW-MUTATION", threadCount));
    StageManager.registerStage(StorageService.readStage_,
        new MultiThreadedStage("ROW-READ", threadCount));
    /* Stage for handling the HTTP messages. */
    StageManager.registerStage(HttpConnection.httpStage_,
        new SingleThreadedStage("HTTP-REQUEST"));

    if (DatabaseDescriptor.isRackAware())
      nodePicker_ = new RackAwareStrategy(tokenMetadata_);
    else
      nodePicker_ = new RackUnawareStrategy(tokenMetadata_);
  }

  private void reportToZookeeper() throws Throwable {
    try {
      zk_ = new ZooKeeper(DatabaseDescriptor.getZkAddress(), DatabaseDescriptor
          .getZkSessionTimeout(), new Watcher() {
        public void process(WatcherEvent we) {
          String path = "/Cassandra/" + DatabaseDescriptor.getClusterName()
              + "/Leader";
          String eventPath = we.getPath();
          logger_.debug("PROCESS EVENT : " + eventPath);
          if (eventPath != null && (eventPath.indexOf(path) != -1)) {
            logger_.debug("Signalling the leader instance ...");
            LeaderElector.instance().signal();
          }
        }
      });

      Stat stat = zk_.exists("/", false);
      if (stat != null) {
        stat = zk_.exists("/Cassandra", false);
        if (stat == null) {
          logger_.debug("Creating the Cassandra znode ...");
          zk_.create("/Cassandra", new byte[0], Ids.OPEN_ACL_UNSAFE, 0);
        }

        String path = "/Cassandra/" + DatabaseDescriptor.getClusterName();
        stat = zk_.exists(path, false);
        if (stat == null) {
          logger_.debug("Creating the cluster znode " + path);
          zk_.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, 0);
        }

        /* Create the Leader, Locks and Misc znode */
        stat = zk_.exists(path + "/Leader", false);
        if (stat == null) {
          logger_.debug("Creating the leader znode " + path);
          zk_.create(path + "/Leader", new byte[0], Ids.OPEN_ACL_UNSAFE, 0);
        }

        stat = zk_.exists(path + "/Locks", false);
        if (stat == null) {
          logger_.debug("Creating the locks znode " + path);
          zk_.create(path + "/Locks", new byte[0], Ids.OPEN_ACL_UNSAFE, 0);
        }

        stat = zk_.exists(path + "/Misc", false);
        if (stat == null) {
          logger_.debug("Creating the misc znode " + path);
          zk_.create(path + "/Misc", new byte[0], Ids.OPEN_ACL_UNSAFE, 0);
        }
      }
    } catch (KeeperException ke) {
      LogUtil.throwableToString(ke);
      /* do the re-initialize again. */
      reportToZookeeper();
    }
  }

  protected ZooKeeper getZooKeeperHandle() {
    return zk_;
  }

  public boolean isLeader(EndPoint endpoint) {
    EndPoint leader = getLeader();
    return leader.equals(endpoint);
  }

  public EndPoint getLeader() {
    return LeaderElector.instance().getLeader();
  }

  public void registerComponentForShutdown(IComponentShutdown component) {
    components_.add(component);
  }

  public void registerExternalVerbHandler(String verb, IVerbHandler verbHandler) {
    MessagingService.getMessagingInstance().registerVerbHandlers(verb,
        verbHandler);
  }

  public void start() throws Throwable {
    storageMetadata_ = DBManager.instance().start();

    /* Set up TCP endpoint */
    tcpAddr_ = new EndPoint(DatabaseDescriptor.getStoragePort());
    /* Set up UDP endpoint */
    udpAddr_ = new EndPoint(DatabaseDescriptor.getControlPort());
    /* Listen for application messages */
    MessagingService.getMessagingInstance().listen(tcpAddr_, false);
    /* Listen for control messages */
    MessagingService.getMessagingInstance().listenUDP(udpAddr_);
    /* Listen for HTTP messages */
    MessagingService.getMessagingInstance().listen(
        new EndPoint(DatabaseDescriptor.getHttpPort()), true);
    /* start the analytics context package */
    AnalyticsContext.instance().start();
    /*
     * report our existence to ZooKeeper instance and start the leader election
     * service
     */
    // reportToZookeeper();
    // LeaderElector.instance().start();
    /* Start the storage load balancer */
    storageLoadBalancer_.start();
    /* Register with the Gossiper for EndPointState notifications */
    Gossiper.instance().register(this);
    /*
     * Start the gossiper with the generation # retrieved from the System table
     */
    Gossiper.instance().start(udpAddr_, storageMetadata_.getGeneration());
    /* Set up the request sampler */
    sampler_ = new RequestCountSampler();
    /* Make sure this token gets gossiped around. */
    tokenMetadata_.update(storageMetadata_.getStorageId(),
        StorageService.tcpAddr_);
    Gossiper.instance().addApplicationState(StorageService.nodeId_,
        new ApplicationState(storageMetadata_.getStorageId().toString()));
  }

  public void killMe() throws Throwable {
    isShutdown_.set(true);
    /*
     * Shutdown the Gossiper to stop responding/sending Gossip messages. This
     * causes other nodes to detect you as dead and starting hinting data for
     * the local endpoint.
     */
    Gossiper.instance().shutdown();
    final long nodeDeadDetectionTime = 25000L;
    Thread.sleep(nodeDeadDetectionTime);
    /* Now perform a force flush of the table */
    String table = DatabaseDescriptor.getTables().get(0);
    Table.open(table).flush(false);
    /* Now wait for the flush to complete */
    Thread.sleep(nodeDeadDetectionTime);
    /* Shutdown all other components */
    StorageService.instance().shutdown();
  }

  public boolean isShutdown() {
    return isShutdown_.get();
  }

  public void shutdown() {
    bootStrapper_.shutdownNow();
    /* shut down all stages */
    StageManager.shutdown();
    /* shut down the messaging service */
    MessagingService.shutdown();
    /* shut down all memtables */
    Memtable.shutdown();
    /* shut down the request count sampler */
    RequestCountSampler.shutdown();
    /* shut down the cleaner thread in FileUtils */
    FileUtils.shutdown();

    /* shut down all registered components */
    for (IComponentShutdown component : components_) {
      component.shutdown();
    }
  }

  public TokenMetadata getTokenMetadata() {
    return tokenMetadata_.cloneMe();
  }

  /* TODO: remove later */
  public void updateTokenMetadata(BigInteger token, EndPoint endpoint) {
    tokenMetadata_.update(token, endpoint);
  }

  public IEndPointSnitch getEndPointSnitch() {
    return endPointSnitch_;
  }

  /*
   * Given an EndPoint this method will report if the endpoint is in the same
   * data center as the local storage endpoint.
   */
  public boolean isInSameDataCenter(EndPoint endpoint) throws IOException {
    return endPointSnitch_
        .isInSameDataCenter(StorageService.tcpAddr_, endpoint);
  }

  /*
   * This method performs the requisite operations to make sure that the N
   * replicas are in sync. We do this in the background when we do not care much
   * about consistency.
   */
  public void doConsistencyCheck(Row row, List<EndPoint> endpoints,
      String columnFamily, int start, int count) {
    Runnable consistencySentinel = new ConsistencyManager(row.cloneMe(),
        endpoints, columnFamily, start, count);
    consistencyManager_.submit(consistencySentinel);
  }

  public void doConsistencyCheck(Row row, List<EndPoint> endpoints,
      String columnFamily, long sinceTimestamp) {
    Runnable consistencySentinel = new ConsistencyManager(row.cloneMe(),
        endpoints, columnFamily, sinceTimestamp);
    consistencyManager_.submit(consistencySentinel);
  }

  public void doConsistencyCheck(Row row, List<EndPoint> endpoints,
      String columnFamily, List<String> columns) {
    Runnable consistencySentinel = new ConsistencyManager(row.cloneMe(),
        endpoints, columnFamily, columns);
    consistencyManager_.submit(consistencySentinel);
  }

  /*
   * This method displays all the ranges and the replicas that are responsible
   * for the individual ranges. The format of this string is the following:
   * 
   * R1 : A B C R2 : D E F R3 : G H I
   */
  public String showTheRing() {
    StringBuilder sb = new StringBuilder();
    /* Get the token to endpoint map. */
    Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_
        .cloneTokenEndPointMap();
    Set<BigInteger> tokens = tokenToEndPointMap.keySet();
    /* All the ranges for the tokens */
    Range[] ranges = getAllRanges(tokens);
    Map<Range, List<EndPoint>> oldRangeToEndPointMap = constructRangeToEndPointMap(ranges);

    Set<Range> rangeSet = oldRangeToEndPointMap.keySet();
    for (Range range : rangeSet) {
      sb.append(range);
      sb.append(" : ");

      List<EndPoint> replicas = oldRangeToEndPointMap.get(range);
      for (EndPoint replica : replicas) {
        sb.append(replica);
        sb.append(" ");
      }
      sb.append(System.getProperty("line.separator"));
    }
    return sb.toString();
  }

  public Map<Range, List<EndPoint>> getRangeToEndPointMap() {
    /* Get the token to endpoint map. */
    Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_
        .cloneTokenEndPointMap();
    Set<BigInteger> tokens = tokenToEndPointMap.keySet();
    /* All the ranges for the tokens */
    Range[] ranges = getAllRanges(tokens);
    Map<Range, List<EndPoint>> oldRangeToEndPointMap = constructRangeToEndPointMap(ranges);

    return oldRangeToEndPointMap;
  }

  /**
   * Construct the range to endpoint mapping based on the true view of the
   * world.
   * 
   * @param ranges
   * @return mapping of ranges to the replicas responsible for them.
   */
  public Map<Range, List<EndPoint>> constructRangeToEndPointMap(Range[] ranges) {
    logger_.debug("Constructing range to endpoint map ...");
    Map<Range, List<EndPoint>> rangeToEndPointMap = new HashMap<Range, List<EndPoint>>();
    for (Range range : ranges) {
      EndPoint[] endpoints = getNStorageEndPoint(range.right());
      rangeToEndPointMap.put(range, new ArrayList<EndPoint>(Arrays
          .asList(endpoints)));
    }
    logger_.debug("Done constructing range to endpoint map ...");
    return rangeToEndPointMap;
  }

  /**
   * Construct the range to endpoint mapping based on the view as dictated by
   * the mapping of token to endpoints passed in.
   * 
   * @param ranges
   * @param tokenToEndPointMap
   *          mapping of token to endpoints.
   * @return mapping of ranges to the replicas responsible for them.
   */
  public Map<Range, List<EndPoint>> constructRangeToEndPointMap(Range[] ranges,
      Map<BigInteger, EndPoint> tokenToEndPointMap) {
    logger_.debug("Constructing range to endpoint map ...");
    Map<Range, List<EndPoint>> rangeToEndPointMap = new HashMap<Range, List<EndPoint>>();
    for (Range range : ranges) {
      EndPoint[] endpoints = getNStorageEndPoint(range.right(),
          tokenToEndPointMap);
      rangeToEndPointMap.put(range, new ArrayList<EndPoint>(Arrays
          .asList(endpoints)));
    }
    logger_.debug("Done constructing range to endpoint map ...");
    return rangeToEndPointMap;
  }

  /**
   * Construct a mapping from endpoint to ranges that endpoint is responsible
   * for.
   * 
   * @return the mapping from endpoint to the ranges it is responsible for.
   */
  public Map<EndPoint, List<Range>> constructEndPointToRangesMap() {
    Map<EndPoint, List<Range>> endPointToRangesMap = new HashMap<EndPoint, List<Range>>();
    Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_
        .cloneTokenEndPointMap();
    Collection<EndPoint> mbrs = tokenToEndPointMap.values();
    for (EndPoint mbr : mbrs) {
      endPointToRangesMap.put(mbr, getRangesForEndPoint(mbr));
    }
    return endPointToRangesMap;
  }

  /**
   * Get the estimated disk space of the target endpoint in its primary range.
   * 
   * @param target
   *          whose primary range we are interested in.
   * @return disk space of the target in the primary range.
   */
  private double getDiskSpaceForPrimaryRange(EndPoint target) {
    double primaryDiskSpace = 0d;
    Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_
        .cloneTokenEndPointMap();
    Set<BigInteger> tokens = tokenToEndPointMap.keySet();
    Range[] allRanges = getAllRanges(tokens);
    Arrays.sort(allRanges);
    /* Mapping from Range to its ordered position on the ring */
    Map<Range, Integer> rangeIndex = new HashMap<Range, Integer>();
    for (int i = 0; i < allRanges.length; ++i) {
      rangeIndex.put(allRanges[i], i);
    }
    /* Get the coefficients for the equations */
    List<double[]> equations = new ArrayList<double[]>();
    /* Get the endpoint to range map */
    Map<EndPoint, List<Range>> endPointToRangesMap = constructEndPointToRangesMap();
    Set<EndPoint> eps = endPointToRangesMap.keySet();

    for (EndPoint ep : eps) {
      List<Range> ranges = endPointToRangesMap.get(ep);
      double[] equation = new double[allRanges.length];
      for (Range range : ranges) {
        int index = rangeIndex.get(range);
        equation[index] = 1;
      }
      equations.add(equation);
    }
    double[][] coefficients = equations.toArray(new double[0][0]);

    /* Get the constants which are the aggregate disk space for each endpoint */
    double[] constants = new double[allRanges.length];
    int index = 0;
    for (EndPoint ep : eps) {
      /* reset the port back to control port */
      ep.setPort(DatabaseDescriptor.getControlPort());
      String lInfo = null;
      if (ep.equals(StorageService.udpAddr_))
        lInfo = getLoadInfo();
      else
        lInfo = getLoadInfo(ep);
      LoadInfo li = new LoadInfo(lInfo);
      constants[index++] = FileUtils.stringToFileSize(li.diskSpace());
    }

    RealMatrix matrix = new RealMatrixImpl(coefficients);
    double[] solutions = matrix.solve(constants);
    Range primaryRange = getPrimaryRangeForEndPoint(target);
    primaryDiskSpace = solutions[rangeIndex.get(primaryRange)];
    return primaryDiskSpace;
  }

  /**
   * This is very dangerous. This is used only on the client side to set up the
   * client library. This is then used to find the appropriate nodes to route
   * the key to.
   */
  public void setTokenMetadata(TokenMetadata tokenMetadata) {
    tokenMetadata_ = tokenMetadata;
  }

  /**
   * Called when there is a change in application state. In particular we are
   * interested in new tokens as a result of a new node or an existing node
   * moving to a new location on the ring.
   */
  public void onChange(EndPoint endpoint, EndPointState epState) {
    EndPoint ep = new EndPoint(endpoint.getHost(), DatabaseDescriptor
        .getStoragePort());
    /* node identifier for this endpoint on the identifier space */
    ApplicationState nodeIdState = epState
        .getApplicationState(StorageService.nodeId_);
    if (nodeIdState != null) {
      BigInteger newToken = new BigInteger(nodeIdState.getState());
      logger_.debug("CHANGE IN STATE FOR " + endpoint + " - has token "
          + nodeIdState.getState());
      BigInteger oldToken = tokenMetadata_.getToken(ep);

      if (oldToken != null) {
        /*
         * If oldToken equals the newToken then the node had crashed and is
         * coming back up again. If oldToken is not equal to the newToken this
         * means that the node is being relocated to another position in the
         * ring.
         */
        if (!oldToken.equals(newToken)) {
          logger_.debug("Relocation for endpoint " + ep);
          tokenMetadata_.update(newToken, ep);
        } else {
          /*
           * This means the node crashed and is coming back up. Deliver the
           * hints that we have for this endpoint.
           */
          logger_.debug("Sending hinted data to " + ep);
          doBootstrap(endpoint, BootstrapMode.HINT);
        }
      } else {
        /*
         * This is a new node and we just update the token map.
         */
        tokenMetadata_.update(newToken, ep);
      }
    } else {
      /*
       * If we are here and if this node is UP and already has an entry in the
       * token map. It means that the node was behind a network partition.
       */
      if (epState.isAlive() && tokenMetadata_.isKnownEndPoint(endpoint)) {
        logger_.debug("EndPoint " + ep
            + " just recovered from a partition. Sending hinted data.");
        doBootstrap(ep, BootstrapMode.HINT);
      }
    }

    /* Check if a bootstrap is in order */
    ApplicationState loadAllState = epState
        .getApplicationState(StorageService.loadAll_);
    if (loadAllState != null) {
      String nodes = loadAllState.getState();
      if (nodes != null) {
        doBootstrap(ep, BootstrapMode.FULL);
      }
    }
  }

  public static BigInteger generateRandomToken() {
    byte[] randomBytes = new byte[24];
    Random random = new Random();
    for (int i = 0; i < 24; i++) {
      randomBytes[i] = (byte) (31 + random.nextInt(256 - 31));
    }
    return hash(new String(randomBytes));
  }

  /**
   * This method is called by the Load Balancing module and the Bootstrap
   * module. Here we receive a Counting Bloom Filter which we merge into the
   * counter.
   */
  public void sample(RequestCountSampler.Cardinality cardinality) {
    if (cardinality == null)
      return;
    sampler_.add(cardinality);
  }

  /**
   * Get the count of primary keys from the sampler.
   */
  public String getLoadInfo() {
    long diskSpace = FileUtils.getUsedDiskSpace();
    LoadInfo li = new LoadInfo(sampler_.count(), diskSpace);
    return li.toString();
  }

  /**
   * Get the primary count info for this endpoint. This is gossiped around and
   * cached in the StorageLoadBalancer.
   */
  public String getLoadInfo(EndPoint ep) {
    LoadInfo li = storageLoadBalancer_.getLoad(ep);
    return (li == null) ? "N/A" : li.toString();
  }

  /**
   * Get the endpoint that has the largest primary count.
   * 
   * @return
   */
  EndPoint getEndPointWithLargestPrimaryCount() {
    Set<EndPoint> allMbrs = Gossiper.instance().getAllMembers();
    Map<LoadInfo, EndPoint> loadInfoToEndPointMap = new HashMap<LoadInfo, EndPoint>();
    List<LoadInfo> lInfos = new ArrayList<LoadInfo>();

    for (EndPoint mbr : allMbrs) {
      mbr.setPort(DatabaseDescriptor.getStoragePort());
      LoadInfo li = null;
      if (mbr.equals(StorageService.tcpAddr_)) {
        li = new LoadInfo(getLoadInfo());
        lInfos.add(li);
      } else {
        li = storageLoadBalancer_.getLoad(mbr);
        lInfos.add(li);
      }
      loadInfoToEndPointMap.put(li, mbr);
    }

    Collections.sort(lInfos, new LoadInfo.PrimaryCountComparator());
    return loadInfoToEndPointMap.get(lInfos.get(lInfos.size() - 1));
  }

  /**
   * This method will sample the key into the request count sampler.
   */
  public void sample(String key) {
    if (isPrimary(key)) {
      sampler_.sample(key);
    }
  }

  /**
   * This method will delete the key from the request count sampler.
   */
  public void delete(String key) {
    if (isPrimary(key)) {
      sampler_.delete(key);
    }
  }

  /*
   * This method updates the token on disk and modifies the cached
   * StorageMetadata instance. This is only for the local endpoint.
   */
  public void updateToken(BigInteger token) throws IOException {
    /* update the token on disk */
    SystemTable.openSystemTable(SystemTable.name_).updateToken(token);
    /* Update the storageMetadata cache */
    storageMetadata_.setStorageId(token);
    /* Update the token maps */
    /* Get the old token. This needs to be removed. */
    tokenMetadata_.update(token, StorageService.tcpAddr_);
    /* Gossip this new token for the local storage instance */
    Gossiper.instance().addApplicationState(StorageService.nodeId_,
        new ApplicationState(token.toString()));
  }

  /*
   * This method removes the state associated with this endpoint from the
   * TokenMetadata instance.
   * 
   * param@ endpoint remove the token state associated with this endpoint.
   */
  public void removeTokenState(EndPoint endpoint) {
    tokenMetadata_.remove(endpoint);
    /* Remove the state from the Gossiper */
    Gossiper.instance().removeFromMembership(endpoint);
  }

  /*
   * This method is invoked by the Loader process to force the node to move from
   * its current position on the token ring, to a position to be determined
   * based on the keys. This will help all nodes to start off perfectly load
   * balanced. The array passed in is evaluated as follows by the loader
   * process: If there are 10 keys in the system and a totality of 5 nodes then
   * each node needs to have 2 keys i.e the array is made up of every 2nd key in
   * the total list of keys.
   */
  public void relocate(String[] keys) throws IOException {
    if (keys.length > 0) {
      isLoadState_ = true;
      BigInteger token = tokenMetadata_.getToken(StorageService.tcpAddr_);
      Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_
          .cloneTokenEndPointMap();
      BigInteger[] tokens = tokenToEndPointMap.keySet().toArray(
          new BigInteger[0]);
      Arrays.sort(tokens);
      int index = Arrays.binarySearch(tokens, token)
          * (keys.length / tokens.length);
      BigInteger newToken = hash(keys[index]);
      /* update the token */
      updateToken(newToken);
    }
  }

  /*
   * This is used to indicate that this node is done with the loading of data.
   */
  public void resetLoadState() {
    isLoadState_ = false;
  }

  /**
   * This method takes a colon separated string of nodes that need to be
   * bootstrapped. It is also used to filter some source of data. Suppose the
   * nodes to be bootstrapped are A, B and C. Then <i>allNodes</i> must be
   * specified as A:B:C.
   * 
   */
  private void doBootstrap(String nodes) {
    String[] allNodesAndFilter = nodes.split("-");
    String nodesToLoad = null;
    String filterSources = null;

    if (allNodesAndFilter.length == 2) {
      nodesToLoad = allNodesAndFilter[0];
      filterSources = allNodesAndFilter[1];
    } else {
      nodesToLoad = allNodesAndFilter[0];
    }
    String[] allNodes = nodesToLoad.split(":");
    EndPoint[] endpoints = new EndPoint[allNodes.length];
    BigInteger[] tokens = new BigInteger[allNodes.length];

    for (int i = 0; i < allNodes.length; ++i) {
      endpoints[i] = new EndPoint(allNodes[i].trim(), DatabaseDescriptor
          .getStoragePort());
      tokens[i] = tokenMetadata_.getToken(endpoints[i]);
    }

    /* Start the bootstrap algorithm */
    if (filterSources == null)
      bootStrapper_.submit(new BootStrapper(endpoints, tokens));
    else {
      String[] allFilters = filterSources.split(":");
      EndPoint[] filters = new EndPoint[allFilters.length];
      for (int i = 0; i < allFilters.length; ++i) {
        filters[i] = new EndPoint(allFilters[i].trim(), DatabaseDescriptor
            .getStoragePort());
      }
      bootStrapper_.submit(new BootStrapper(endpoints, tokens, filters));
    }
  }

  /**
   * Starts the bootstrap operations for the specified endpoint. The name of
   * this method is however a misnomer since it does handoff of data to the
   * specified node when it has crashed and come back up, marked as alive after
   * a network partition and also when it joins the ring either as an old node
   * being relocated or as a brand new node.
   */
  public final void doBootstrap(EndPoint endpoint, BootstrapMode mode) {
    switch (mode) {
    case FULL:
      BigInteger token = tokenMetadata_.getToken(endpoint);
      bootStrapper_.submit(new BootStrapper(new EndPoint[] { endpoint },
          new BigInteger[] { token }));
      break;

    case HINT:
      /* Deliver the hinted data to this endpoint. */
      HintedHandOffManager.instance().deliverHints(endpoint);
      break;

    default:
      break;
    }
  }

  /* This methods belong to the MBean interface */

  public long getRequestHandled() {
    return sampler_.count();
  }

  public String getToken(EndPoint ep) {
    EndPoint ep2 = new EndPoint(ep.getHost(), DatabaseDescriptor
        .getStoragePort());
    BigInteger token = tokenMetadata_.getToken(ep2);
    return (token == null) ? BigInteger.ZERO.toString() : token.toString();
  }

  public String getToken() {
    return tokenMetadata_.getToken(StorageService.tcpAddr_).toString();
  }

  public void updateToken(String token) {
    try {
      updateToken(new BigInteger(token));
    } catch (IOException ex) {
      logger_.debug(LogUtil.throwableToString(ex));
    }
  }

  public String getLiveNodes() {
    return stringify(Gossiper.instance().getLiveMembers());
  }

  public String getUnreachableNodes() {
    return stringify(Gossiper.instance().getUnreachableMembers());
  }

  /* Helper for the MBean interface */
  private String stringify(Set<EndPoint> eps) {
    StringBuilder sb = new StringBuilder("");
    for (EndPoint ep : eps) {
      sb.append(ep);
      sb.append(" ");
    }
    return sb.toString();
  }

  public void loadAll(String nodes) {
    // Gossiper.instance().addApplicationState(StorageService.loadAll_, new
    // ApplicationState(nodes));
    doBootstrap(nodes);
  }

  public String getAppropriateToken(int count) {
    BigInteger token = BootstrapAndLbHelper.getTokenBasedOnPrimaryCount(count);
    return token.toString();
  }

  public void doGC() {
    List<String> tables = DatabaseDescriptor.getTables();
    for (String tName : tables) {
      Table table = Table.open(tName);
      table.doGC();
    }
  }

  public void forceHandoff(String directories, String host) throws IOException {
    List<File> filesList = new ArrayList<File>();
    String[] sources = directories.split(":");
    for (String source : sources) {
      File directory = new File(source);
      Collections.addAll(filesList, directory.listFiles());
    }

    File[] files = filesList.toArray(new File[0]);
    StreamContextManager.StreamContext[] streamContexts = new StreamContextManager.StreamContext[files.length];
    int i = 0;
    for (File file : files) {
      streamContexts[i] = new StreamContextManager.StreamContext(file
          .getAbsolutePath(), file.length());
      logger_.debug("Stream context metadata " + streamContexts[i]);
      ++i;
    }

    if (files.length > 0) {
      EndPoint target = new EndPoint(host, DatabaseDescriptor.getStoragePort());
      /* Set up the stream manager with the files that need to streamed */
      StreamManager.instance(target).addFilesToStream(streamContexts);
      /* Send the bootstrap initiate message */
      BootstrapInitiateMessage biMessage = new BootstrapInitiateMessage(
          streamContexts);
      Message message = BootstrapInitiateMessage
          .makeBootstrapInitiateMessage(biMessage);
      logger_.debug("Sending a bootstrap initiate message to " + target
          + " ...");
      MessagingService.getMessagingInstance().sendOneWay(message, target);
      logger_.debug("Waiting for transfer to " + target + " to complete");
      StreamManager.instance(target).waitForStreamCompletion();
      logger_.debug("Done with transfer to " + target);
    }
  }

  /* End of MBean interface methods */

  /*
   * This method returns the predecessor of the endpoint ep on the identifier
   * space.
   */
  EndPoint getPredecessor(EndPoint ep) {
    BigInteger token = tokenMetadata_.getToken(ep);
    Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_
        .cloneTokenEndPointMap();
    List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToEndPointMap
        .keySet());
    Collections.sort(tokens);
    int index = Collections.binarySearch(tokens, token);
    EndPoint predecessor = (index == 0) ? tokenToEndPointMap.get(tokens
        .get(tokens.size() - 1)) : tokenToEndPointMap.get(tokens.get(--index));
    return predecessor;
  }

  /*
   * This method returns the successor of the endpoint ep on the identifier
   * space.
   */
  public EndPoint getSuccessor(EndPoint ep) {
    BigInteger token = tokenMetadata_.getToken(ep);
    Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_
        .cloneTokenEndPointMap();
    List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToEndPointMap
        .keySet());
    Collections.sort(tokens);
    int index = Collections.binarySearch(tokens, token);
    EndPoint successor = (index == (tokens.size() - 1)) ? tokenToEndPointMap
        .get(tokens.get(0)) : tokenToEndPointMap.get(tokens.get(++index));
    return successor;
  }

  /**
   * This method returns the range handled by this node.
   */
  public Range getMyRange() {
    BigInteger myToken = tokenMetadata_.getToken(StorageService.tcpAddr_);
    Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_
        .cloneTokenEndPointMap();
    List<BigInteger> allTokens = new ArrayList<BigInteger>(tokenToEndPointMap
        .keySet());
    Collections.sort(allTokens);
    int index = Collections.binarySearch(allTokens, myToken);
    /* Calculate the lhs for the range */
    BigInteger lhs = (index == 0) ? allTokens.get(allTokens.size() - 1)
        : allTokens.get(index - 1);
    return new Range(lhs, myToken);
  }

  /**
   * Get the primary range for the specified endpoint.
   * 
   * @param ep
   *          endpoint we are interested in.
   * @return range for the specified endpoint.
   */
  public Range getPrimaryRangeForEndPoint(EndPoint ep) {
    BigInteger right = tokenMetadata_.getToken(ep);
    EndPoint predecessor = getPredecessor(ep);
    BigInteger left = tokenMetadata_.getToken(predecessor);
    return new Range(left, right);
  }

  /**
   * Get all ranges an endpoint is responsible for.
   * 
   * @param ep
   *          endpoint we are interested in.
   * @return ranges for the specified endpoint.
   */
  List<Range> getRangesForEndPoint(EndPoint ep) {
    List<Range> ranges = new ArrayList<Range>();
    ranges.add(getPrimaryRangeForEndPoint(ep));

    EndPoint predecessor = ep;
    int count = DatabaseDescriptor.getReplicationFactor() - 1;
    for (int i = 0; i < count; ++i) {
      predecessor = getPredecessor(predecessor);
      ranges.add(getPrimaryRangeForEndPoint(predecessor));
    }

    return ranges;
  }

  /**
   * Get all ranges that span the ring given a set of tokens. All ranges are in
   * sorted order of ranges.
   */
  public Range[] getAllRanges(Set<BigInteger> tokens) {
    List<Range> ranges = new ArrayList<Range>();
    List<BigInteger> allTokens = new ArrayList<BigInteger>(tokens);
    Collections.sort(allTokens);
    int size = allTokens.size();
    for (int i = 1; i < size; ++i) {
      Range range = new Range(allTokens.get(i - 1), allTokens.get(i));
      ranges.add(range);
    }
    Range range = new Range(allTokens.get(size - 1), allTokens.get(0));
    ranges.add(range);
    return ranges.toArray(new Range[0]);
  }

  /**
   * Get all ranges that span the ring given a set of endpoints.
   */
  public Range[] getPrimaryRangesForEndPoints(Set<EndPoint> endpoints) {
    List<Range> allRanges = new ArrayList<Range>();
    for (EndPoint endpoint : endpoints) {
      allRanges.add(getPrimaryRangeForEndPoint(endpoint));
    }
    return allRanges.toArray(new Range[0]);
  }

  /**
   * This method returns the endpoint that is responsible for storing the
   * specified key.
   * 
   * param @ key - key for which we need to find the endpoint return value - the
   * endpoint responsible for this key
   */
  public EndPoint getPrimary(String key) {
    EndPoint endpoint = StorageService.tcpAddr_;
    BigInteger token = hash(key);
    Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_
        .cloneTokenEndPointMap();
    List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToEndPointMap
        .keySet());
    if (tokens.size() > 0) {
      Collections.sort(tokens);
      int index = Collections.binarySearch(tokens, token);
      if (index >= 0) {
        /*
         * retrieve the endpoint based on the token at this index in the tokens
         * list
         */
        endpoint = tokenToEndPointMap.get(tokens.get(index));
      } else {
        index = (index + 1) * (-1);
        if (index < tokens.size())
          endpoint = tokenToEndPointMap.get(tokens.get(index));
        else
          endpoint = tokenToEndPointMap.get(tokens.get(0));
      }
    }
    return endpoint;
  }

  /**
   * This method determines whether the local endpoint is the primary for the
   * given key.
   * 
   * @param key
   * @return true if the local endpoint is the primary replica.
   */
  public boolean isPrimary(String key) {
    EndPoint endpoint = getPrimary(key);
    return StorageService.tcpAddr_.equals(endpoint);
  }

  /**
   * This method determines whether the target endpoint is the primary for the
   * given key.
   * 
   * @param key
   * @param target
   *          the target enpoint
   * @return true if the local endpoint is the primary replica.
   */
  public boolean isPrimary(String key, EndPoint target) {
    EndPoint endpoint = getPrimary(key);
    return target.equals(endpoint);
  }

  /**
   * This method determines whether the local endpoint is the seondary replica
   * for the given key.
   * 
   * @param key
   * @return true if the local endpoint is the secondary replica.
   */
  public boolean isSecondary(String key) {
    EndPoint[] topN = getNStorageEndPoint(key);
    if (topN.length < DatabaseDescriptor.getReplicationFactor())
      return false;
    return topN[1].equals(StorageService.tcpAddr_);
  }

  /**
   * This method determines whether the local endpoint is the seondary replica
   * for the given key.
   * 
   * @param key
   * @return true if the local endpoint is the tertiary replica.
   */
  public boolean isTertiary(String key) {
    EndPoint[] topN = getNStorageEndPoint(key);
    if (topN.length < DatabaseDescriptor.getReplicationFactor())
      return false;
    return topN[2].equals(StorageService.tcpAddr_);
  }

  /**
   * This method determines if the local endpoint is in the topN of N nodes
   * passed in.
   */
  public boolean isInTopN(String key) {
    EndPoint[] topN = getNStorageEndPoint(key);
    for (EndPoint ep : topN) {
      if (ep.equals(StorageService.tcpAddr_))
        return true;
    }
    return false;
  }

  /**
   * This method returns the N endpoints that are responsible for storing the
   * specified key i.e for replication.
   * 
   * param @ key - key for which we need to find the endpoint return value - the
   * endpoint responsible for this key
   */
  public EndPoint[] getNStorageEndPoint(String key) {
    BigInteger token = hash(key);
    return nodePicker_.getStorageEndPoints(token);
  }

  /**
   * This method attempts to return N endpoints that are responsible for storing
   * the specified key i.e for replication.
   * 
   * param @ key - key for which we need to find the endpoint return value - the
   * endpoint responsible for this key
   */
  public List<EndPoint> getNLiveStorageEndPoint(String key) {
    List<EndPoint> liveEps = new ArrayList<EndPoint>();
    EndPoint[] endpoints = getNStorageEndPoint(key);

    for (EndPoint endpoint : endpoints) {
      if (FailureDetector.instance().isAlive(endpoint))
        liveEps.add(endpoint);
    }

    return liveEps;
  }

  /**
   * This method returns the N endpoints that are responsible for storing the
   * specified key i.e for replication.
   * 
   * param @ key - key for which we need to find the endpoint return value - the
   * endpoint responsible for this key
   */
  public Map<EndPoint, EndPoint> getNStorageEndPointMap(String key) {
    BigInteger token = hash(key);
    return nodePicker_.getHintedStorageEndPoints(token);
  }

  /**
   * This method returns the N endpoints that are responsible for storing the
   * specified key i.e for replication. But it makes sure that the N endpoints
   * that are returned are live as reported by the FD. It returns the hint
   * information if some nodes in the top N are not live.
   * 
   * param @ key - key for which we need to find the endpoint return value - the
   * endpoint responsible for this key
   */
  public Map<EndPoint, EndPoint> getNHintedStorageEndPoint(String key) {
    BigInteger token = hash(key);
    return nodePicker_.getHintedStorageEndPoints(token);
  }

  /**
   * This method returns the N endpoints that are responsible for storing the
   * specified token i.e for replication.
   * 
   * param @ token - position on the ring
   */
  public EndPoint[] getNStorageEndPoint(BigInteger token) {
    return nodePicker_.getStorageEndPoints(token);
  }

  /**
   * This method returns the N endpoints that are responsible for storing the
   * specified token i.e for replication and are based on the token to endpoint
   * mapping that is passed in.
   * 
   * param @ token - position on the ring param @ tokens - w/o the following
   * tokens in the token list
   */
  protected EndPoint[] getNStorageEndPoint(BigInteger token,
      Map<BigInteger, EndPoint> tokenToEndPointMap) {
    return nodePicker_.getStorageEndPoints(token, tokenToEndPointMap);
  }

  /**
   * This method returns the N endpoints that are responsible for storing the
   * specified key i.e for replication. But it makes sure that the N endpoints
   * that are returned are live as reported by the FD. It returns the hint
   * information if some nodes in the top N are not live.
   * 
   * param @ token - position on the ring
   */
  public Map<EndPoint, EndPoint> getNHintedStorageEndPoint(BigInteger token) {
    return nodePicker_.getHintedStorageEndPoints(token);
  }

  /**
   * This function finds the most suitable endpoint given a key. It checks for
   * loclity and alive test.
   */
  protected EndPoint findSuitableEndPoint(String key) throws IOException {
    EndPoint[] endpoints = getNStorageEndPoint(key);
    for (EndPoint endPoint : endpoints) {
      if (endPoint.equals(StorageService.getLocalStorageEndPoint())) {
        return endPoint;
      }
    }
    int j = 0;
    for (; j < endpoints.length; ++j) {
      if (StorageService.instance().isInSameDataCenter(endpoints[j])
          && FailureDetector.instance().isAlive(endpoints[j])) {
        logger_.debug("EndPoint " + endpoints[j]
            + " is in the same data center as local storage endpoint.");
        return endpoints[j];
      }
    }
    // We have tried to be really nice but looks like theer are no servers
    // in the local data center that are alive and can service this request so
    // just send it to teh first alive guy and see if we get anything.
    j = 0;
    for (; j < endpoints.length; ++j) {
      if (FailureDetector.instance().isAlive(endpoints[j])) {
        logger_.debug("EndPoint " + endpoints[j]
            + " is alive so get data from it.");
        return endpoints[j];
      }
    }
    return null;
  }
}
