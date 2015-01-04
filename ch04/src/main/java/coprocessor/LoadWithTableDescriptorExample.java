package coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

// cc LoadWithTableDescriptorExample Example region observer checking for special get requests
// vv LoadWithTableDescriptorExample
public class LoadWithTableDescriptorExample {

  public static void main(String[] args) throws IOException, URISyntaxException {
    Configuration conf = HBaseConfiguration.create();
    // ^^ LoadWithTableDescriptorExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    // vv LoadWithTableDescriptorExample

    Path path = new Path("hdfs://192.168.222.129:9000/hbase-book-ch04-1.0.jar"); // co LoadWithTableDescriptorExample-1-Path Get the location of the JAR file containing the coprocessor implementation.

    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testtable")); // co LoadWithTableDescriptorExample-2-Define Define a table descriptor.
    htd.addFamily(new HColumnDescriptor("colfam1"));
    htd.addFamily(new HColumnDescriptor("colfam2"));
    htd.setValue("COPROCESSOR$1", path.toString() +
      "|" + RowCounterEndpointExample.class.getCanonicalName() + // co LoadWithTableDescriptorExample-3-AddCP Add the coprocessor definition to the descriptor.
      "|" + Coprocessor.PRIORITY_USER);

    HBaseAdmin admin = new HBaseAdmin(conf); // co LoadWithTableDescriptorExample-4-Admin Instantiate an administrative API to the cluster and add the table.
    admin.createTable(htd);

    System.out.println(admin.getTableDescriptor(Bytes.toBytes("testtable"))); // co LoadWithTableDescriptorExample-5-Check Verify if the definition has been applied as expected.
  }
}
// ^^ LoadWithTableDescriptorExample
