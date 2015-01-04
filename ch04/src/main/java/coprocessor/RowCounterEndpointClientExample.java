package coprocessor;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

import coprocessor.endpoints.generated.RowCounterEndpointProtos.CountRequest;
import coprocessor.endpoints.generated.RowCounterEndpointProtos.CountResponse;
import coprocessor.endpoints.generated.RowCounterEndpointProtos.RowCountService;
import util.HBaseHelper;

public class RowCounterEndpointClientExample {
	public static void main(String[] args) throws ServiceException, Throwable {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		//helper.dropTable("testtable");
		//helper.createTable("testtable", "colfam1", "colfam2");
		System.out.println("Adding rows to table...");
		helper.fillTable("testtable", 1, 100, 100, "colfam1", "colfam2");

		HTable table = new HTable(conf, "testtable");

		final CountRequest request = CountRequest.getDefaultInstance();
		
		final Batch.Call<RowCountService, Long> call =new Batch.Call<RowCountService, Long>() {
			public Long call(RowCountService counter)
					throws IOException {
				ServerRpcController controller = new ServerRpcController();
				BlockingRpcCallback<CountResponse> rpcCallback = new BlockingRpcCallback<CountResponse>();
				counter.getRowCount(controller, request, rpcCallback);
				CountResponse response = rpcCallback.get();
				if (controller.failedOnException()) {
					throw controller.getFailedOn();
				}
				return (response != null && response.hasCount()) ? response
						.getCount() : 0;
			}
		};
		
		Map<byte[], Long> results = table.coprocessorService(
				RowCountService.class, null, null, call);
		
		for(byte[] b : results.keySet()){
			System.err.println(Bytes.toString(b) + ":" + results.get(b));
		} 
	}
}
