package filters;

// cc CustomFilter Implements a filter that lets certain rows pass
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implements a custom filter for HBase. It takes a value and compares
 * it with every value in each KeyValue checked. Once there is a match
 * the entire row is passed, otherwise filtered out.
 */
// vv CustomFilter
public class CustomFilter extends FilterBase{

  private byte[] value = null;
  private boolean filterRow = true;

  public CustomFilter() {
    super();
  }

  public CustomFilter(byte[] value) {
    this.value = value; // co CustomFilter-1-SetValue Set the value to compare against.
  }

  @Override
  public void reset() {
    this.filterRow = true; // co CustomFilter-2-Reset Reset filter flag for each new row being tested.
  }

  @Override
  public ReturnCode filterKeyValue(Cell kv) {
    if (Bytes.compareTo(value, kv.getValueArray()) == 0) {
      filterRow = false; // co CustomFilter-3-Filter When there is a matching value, then let the row pass.
    }
    return ReturnCode.INCLUDE; // co CustomFilter-4-Include Always include, since the final decision is made later.
  }

  @Override
  public boolean filterRow() {
    return filterRow; // co CustomFilter-5-FilterRow Here the actual decision is taking place, based on the flag status.
  }
}
// ^^ CustomFilter
