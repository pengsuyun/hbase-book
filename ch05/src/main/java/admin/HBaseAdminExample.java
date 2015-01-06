package admin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseAdminExample {

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration configuration = HBaseConfiguration.create();
		
		HBaseAdmin baseAdmin = new HBaseAdmin(configuration);
		
		System.err.println("baseAdmin.getClusterStatus():"+baseAdmin.getClusterStatus());
		System.err.println("baseAdmin.isMasterRunning():"+baseAdmin.isMasterRunning());
		System.err.println("baseAdmin.getConnection():"+baseAdmin.getConnection());
		baseAdmin.close();
	}

}
