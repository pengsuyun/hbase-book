package client;

// cc GetExample Example application retrieving data from HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

public class GetExample {

  public static void main(String[] args) throws IOException {
    // vv GetExample
    Configuration conf = HBaseConfiguration.create(); // co GetExample-1-CreateConf Create the configuration.

    // ^^ GetExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    if (!helper.existsTable("testtable")) {
      helper.createTable("testtable", "colfam1");
    }
    // vv GetExample
    HTable table = new HTable(conf, "testtable"); // co GetExample-2-NewTable Instantiate a new table reference.

    System.err.println(table.getRegionLocations());
    
    Get get = new Get(Bytes.toBytes("row1")); // co GetExample-3-NewGet Create get with specific row.

    System.err.println(get.getMaxVersions());
    System.err.println(get.getTimeRange());
    get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")); // co GetExample-4-AddCol Add a column to the get.
    get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2")); // co GetExample-4-AddCol Add a column to the get.
    Result result = table.get(get); // co GetExample-5-DoGet Retrieve row with selected columns from HBase.

    byte[] val = result.getValue(Bytes.toBytes("colfam1"),
      Bytes.toBytes("qual1")); // co GetExample-6-GetValue Get a specific value for the given column.

    System.out.println("Value: " + Bytes.toString(val)); // co GetExample-7-Print Print out the value while converting it back.
    // ^^ GetExample
    
    
    System.err.println();
    Map<byte[],NavigableSet<byte[]>> familyMap = get.getFamilyMap();
    for(byte[] b : familyMap.keySet()){
    	System.err.println(Bytes.toString(b));
    	for(byte[] col : familyMap.get(b)){
    		System.err.println(Bytes.toString(col));
    	}
    }
    System.err.println(get.hasFamilies());
    
    System.err.println(result.containsColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")));
   
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
    for(byte[] b : map.keySet()){
    	System.err.println(Bytes.toString(b)+":");
    	NavigableMap<byte[], NavigableMap<Long, byte[]>> map2 = map.get(b);
    	for(byte[] bb : map2.keySet()){
    		System.err.println(Bytes.toString(bb)+":");
    		for(Map.Entry<Long, byte[]> e : map2.get(bb).entrySet()){
    			System.err.println(e.getKey() + ":" + Bytes.toString(e.getValue()));
    		}
    	}
    }
    
    System.err.println("-----------------------------------");
    
    System.err.println("result:"+result);
  }
}
