/**
 * Created by carybourgeois on 11/8/15.
 */

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.utils.UUIDs;

import java.io.*;
import java.nio.IntBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GetTestData {

    private static Cluster cluster;
    private static Session session;
    private static PreparedStatement ppdStmt;

    public void connect(String node, String userName, String password) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .withCredentials(userName, password)
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
//                .withLoadBalancingPolicy(new DCAwareRoundRobinPolicy.Builder())
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
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

        String cql = "SELECT " +
                        "edge_id, " +
                        "sensor, " +
                        "epoch_min, " +
                        "blobAsBigint(timestampAsBlob(ts)) AS create_ts, " +
                        "blobAsBigint(timestampAsBlob(hub_ts)) AS hub_ts " +
                        "FROM " + tableName + " " +
                        "WHERE edge_id=? AND sensor=? AND epoch_min=?;";

        System.out.println("CQL Command: " + cql);

        ppdStmt = session.prepare(cql);

        ppdStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

    }

    public static void main(String args[]) throws InterruptedException {

        if (args.length != 9) {
            System.err.println("Usage: GetTestData <output filename> <string connectPoint> <string User> <string Password> <string tableName> <int edgeId> <int numSensors> <Long epochMin> <Long numMinutes>");
            return;
        }

        String outFile = args[0];
        String connectPoint = args[1];
        String userName = args[2];
        String password = args[3];
        String tableName = args[4];
        String edgeId = args[5];
        int numSensors = Integer.valueOf(args[6]);
        Long epochMin = Long.valueOf(args[7]);
        Long numMin = Long.valueOf(args[8]);

        GetTestData client = new GetTestData();
        client.connect(connectPoint, userName, password);
        client.prepareSession(tableName);

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), StandardCharsets.UTF_8))) {

            for (int i = 1; i <= numSensors; i++) {
                List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();
                for (Long iMin = 0L; iMin < numMin; iMin++) {
                    Statement stmt = ppdStmt.bind(edgeId, String.valueOf(i), String.valueOf(epochMin + iMin));
                    futures.add(session.executeAsync(stmt));
                }

                for(ResultSetFuture future: futures){
                    ResultSet results = future.getUninterruptibly();
                    int rowCount = 0;
                    String edge_id = "";
                    String sensor = "";
                    String epoch_min = "";
                    for (Row row : results) {
                        rowCount++;

                        edge_id = row.getString("edge_id");
                        sensor = row.getString("sensor");
                        epoch_min = row.getString("epoch_min");
                        Long hub_ts = row.getLong("hub_ts");
                        Long create_ts = row.getLong("create_ts");
                        Long diff = (hub_ts - create_ts);
                        writer.write(row.getString("edge_id") + ";" + row.getString("sensor") + ";" + row.getString("epoch_min") + ";" + create_ts + ";" + hub_ts + ";" + diff + "\n");
                    }
                    System.out.println("edgeId: " + edge_id + " sensor: " + sensor + " epoch_min: " + epoch_min + " Rows written: " + rowCount);
                }


            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        client.close();
    }
}
