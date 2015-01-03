package client;

// cc PutExample Example application inserting data into HBase
// vv PutExample
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;





// ^^ PutExample
import util.HBaseHelper;
// vv PutExample






import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PutExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create(); // co PutExample-1-CreateConf Create the required configuration.

    // ^^ PutExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    // vv PutExample
    HTable table = new HTable(conf, "testtable"); // co PutExample-2-NewTable Instantiate a new client.
    
    System.err.println("table.getWriteBufferSize():"+table.getWriteBufferSize());
    
    //table.setAutoFlushTo(false);
    Put put = new Put(Bytes.toBytes("row1"),System.currentTimeMillis()); // co PutExample-3-NewPut Create put with specific row.
    //put.setAttribute("name", Bytes.toBytes("1111"));
    put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val12")); // co PutExample-4-AddCol1 Add a column, whose name is "colfam1:qual1", to the put.
   // put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
 //     Bytes.toBytes("val21")); // co PutExample-4-AddCol2 Add another column, whose name is "colfam1:qual2", to the put.
  //  put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual4"),
  //  	      Bytes.toBytes("val21"));
    table.put(put); // co PutExample-5-DoPut Store row with column into the HBase table.
    
    //table.flushCommits();
    
    
    Map<byte [], List<KeyValue>> map = put.getFamilyMap();    
    for(byte[] b : map.keySet()){
    	System.err.println(Bytes.toString(b));
    	for(KeyValue keyValue : map.get(b)){
    		System.err.println(Bytes.toString(keyValue.getRow()));
    		System.err.println(keyValue.getTypeByte());
    		
    	}
    	
    	List<KeyValue> list = map.get(b);
    	Collections.sort(list, KeyValue.RAW_COMPARATOR);
    	System.err.println("list:"+list);
    }
    
    List<Cell> cells = put.get(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
    for(Cell cell : cells){
    	System.err.println(cell);
    }
    
    System.err.println(put.has(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")));
    System.err.println(put.has(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2")));
    System.err.println(put.has(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3")));
    
    System.err.println(Bytes.toString(put.getRow()));
    System.err.println(put.getAttributesMap());
    System.err.println(put.getACLStrategy());
    System.err.println(put.getTimeStamp());
    System.err.println(put.numFamilies());
    System.err.println(put.size());
    
    table.close();
  }
}
// ^^ PutExample
