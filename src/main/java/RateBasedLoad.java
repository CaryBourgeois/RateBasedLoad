/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.utils.UUIDs;

import java.text.SimpleDateFormat;
import java.util.*;

class LoadThread implements Runnable {
    private Thread t;

    private int tEdgeId;
    private int tSensorCount;

    private Session threadSession;
    private PreparedStatement threadPpdInsert;

    Random randGen = new Random();

    LoadThread(int edgeId, int sensor_count, Session session, PreparedStatement ppdInsert){
        tEdgeId = edgeId;
        tSensorCount = sensor_count;

        threadSession = session;
        threadPpdInsert = ppdInsert;
    }

    public void run() {

        SimpleDateFormat hdf = new SimpleDateFormat("yyyy-MM-dd HH:MM");
        Calendar calStart = Calendar.getInstance();
        Calendar calStop = Calendar.getInstance();

        calStart.setTime(new Date());

        List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();

        int recCount = 0;

        for (int i = 1; i <= tSensorCount; i++){
            Long ts = System.currentTimeMillis();
            Date dt = new Date();
            dt.setTime(ts);
            String epoch_min = String.valueOf(ts/60000);
            UUID ts_uuid  = UUIDs.startOf(ts);

            futures.add(threadSession.executeAsync(threadPpdInsert.bind(String.valueOf(tEdgeId), String.valueOf(i), epoch_min, dt, randGen.nextDouble(), randGen.nextDouble(), dt)));
            recCount++;
        }

        for(ResultSetFuture future: futures){
            future.getUninterruptibly();
        }

        calStop.setTime(new Date());
        long elapsedMillis = (calStop.getTimeInMillis() - calStart.getTimeInMillis());
        //System.out.println("Wrote " + recCount + " records in " + elapsedMillis + " ms at a rate of " + recCount / (elapsedMillis / 1000.0) + " tps.");
        System.out.println(calStart.getTimeInMillis() + ";" + recCount + ";" + elapsedMillis);
    }

    public void start ()
    {
        //System.out.println("Starting " +  threadName );
        if (t == null)
        {
            t = new Thread (this);
            t.start ();
        }
    }

    public boolean isAlive() {
        return t.isAlive();
    }

}

public class RateBasedLoad {

    private static Cluster cluster;
    private static Session session;
    private static PreparedStatement ppdInsert;

    public void connect(String node) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy
                        .builder()
                        .allowRemoteDCsForLocalConsistencyLevel()
                        .withUsedHostsPerRemoteDc(3)
                        .build()))
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
                .build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
                metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    public void close() {
        cluster.close();
    }

    public void prepareSession(String tableName) {

//        session.execute("CREATE KEYSPACE IF NOT EXISTS demo WITH replication " +
//                "= {'class' : 'NetworkTopologyStrategy', 'us-west' : 3};");
//
//        session.execute("USE demo;");
//
//        session.execute("DROP TABLE IF EXISTS data;");
//        session.execute(
//                "CREATE TABLE IF NOT EXISTS data (" +
//                        "      edge_id text," +
//                        "      sensor text," +
//                        "      epoch_min text," +
//                        "      ts timestamp," +
//                        "      depth double," +
//                        "      value double," +
//                        "      hub_ts timestamp," +
//                        "      PRIMARY KEY ((edge_id, sensor, epoch_min), ts)" +
//                        "  ) WITH CLUSTERING ORDER BY (ts DESC)" +
//                        "  AND compaction = { 'class' :  'LeveledCompactionStrategy'  };"
//        );
        ppdInsert = session.prepare(
                "INSERT INTO " + tableName + " " +
                        "(edge_id, sensor, epoch_min, ts, depth, value, hub_ts) " +
                        " VALUES(?,?,?,?,?,?,?);"
        );

        ppdInsert.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

    }

    public static void main(String args[]) throws InterruptedException {

        if (args.length != 6) {
            System.err.println("Usage: RateBasedLoad <string connectPoint> <table name> <int edgeId> <int numSensors> <int waitMillis> <int numRecords>");
            return;
        }

        String connectPoint = args[0];
        String tableName = args[1];
        int edgeId = Integer.valueOf(args[2]);
        int numSensors = Integer.valueOf(args[3]);
        int waitMillis = Integer.valueOf(args[4]);
        int numRecords = Integer.valueOf(args[5]);

        RateBasedLoad client = new RateBasedLoad();
        client.connect(connectPoint);
        client.prepareSession(tableName);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat tdf = new SimpleDateFormat("HH:mm:ss");
        Calendar calStart = Calendar.getInstance();

        calStart.setTime(new Date());
        System.out.println("Load started: " + sdf.format(calStart.getTime()));
        System.out.println("Epoch Minute at start: " + String.valueOf(System.currentTimeMillis()/60000));

        List<LoadThread> loadThreads = new ArrayList<LoadThread>();

        int numRecordsWritten = 0;

        while (numRecordsWritten < numRecords) {
            LoadThread newThread = new LoadThread(edgeId, numSensors, session, ppdInsert);
            newThread.start();
//            loadThreads.add(newThread);
//
//            for (LoadThread t : loadThreads) {
//                if (!t.isAlive()) {
//                    loadThreads.remove(t);
//                }
//            }

            numRecordsWritten += numSensors;

            Thread.sleep(waitMillis);
        }

        Calendar calStop = Calendar.getInstance();
        calStop.setTime(new Date());
        long elapsedSeconds = (calStop.getTimeInMillis() - calStart.getTimeInMillis()) / 1000;
        String elapsedTime = elapsedSeconds / 3600 + ":" + (elapsedSeconds % 3600) / 60 + ":" + (elapsedSeconds % 60);

        System.out.println("Load stopped: " + sdf.format(calStop.getTime()) + " elapsed time: " + elapsedTime);
        System.out.println("Number of Records Written: " + numRecordsWritten);
        System.out.println("Transactions per second: " + numRecordsWritten/elapsedSeconds);

        client.close();
    }
}
