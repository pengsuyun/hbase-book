package filters;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class KeyOnlyFilterExample {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

	    /*HBaseHelper helper = HBaseHelper.getHelper(conf);
	    helper.dropTable("testtable");
	    helper.createTable("testtable", "colfam1", "colfam2", "colfam3", "colfam4");
	    System.out.println("Adding rows to table...");
	    helper.fillTable("testtable", 1, 10, 2, "colfam1", "colfam2", "colfam3", "colfam4");*/

	    HTable table = new HTable(conf, "testtable");

	    // vv KeyOnlyFilterExample
	    Filter filter = new KeyOnlyFilter(false);
	    
	    Scan scan = new Scan();
	    scan.setFilter(filter);
	    ResultScanner scanner = table.getScanner(scan); // co FamilyFilterExample-2-Scan Scan over table while applying the filter.
	    // ^^ KeyOnlyFilterExample
	    System.out.println("Scanning table... ");
	    // vv KeyOnlyFilterExample
	    for (Result result : scanner) {
	      System.out.println(result);
	    }
	    scanner.close();
	    
	    scan.setFilter(new KeyOnlyFilter(true));
	    ResultScanner scanner1 = table.getScanner(scan); // co FamilyFilterExample-2-Scan Scan over table while applying the filter.
	    // ^^ KeyOnlyFilterExample
	    System.out.println("Scanning table1... ");
	    // vv KeyOnlyFilterExample
	    for (Result result : scanner1) {
	      System.out.println(result);
	    }
	    scanner.close();
	    // ^^ KeyOnlyFilterExample
	}
}
