import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class ColumnAggregationEndpoint implements RegionObserver {

/*
    public RegionScanner preScannerOpen(final ObserverContext e, final Scan scan, final RegionScanner s) throws IOException {

        Filter filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(20)));
        scan.setFilter(filter);
        return s;
    }
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
                         final Get get, final List<Cell> results) throws IOException {

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("quantity"), Bytes.toBytes("quantity")); //family, qualifier

        Filter f1 = new RowFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("quantity")));
        scan.setFilter(f1);

        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("storesales"));


        Filter filter1 = new RowFilter(CompareOperator.LESS_OR_EQUAL, // co RowFilterExample-1-Filter1 Create filter, while specifying the comparison operator and comparator. Here an exact match is needed.
                new BinaryComparator(Bytes.toBytes("quantity")));
        scan.setFilter(filter1);
        ResultScanner scanner1 = table.getScanner(scan);
        // ^^ RowFilterExample
        System.out.println("Scanning table #1...");
        // vv RowFilterExample
        for (Result res : scanner1) {
            System.out.println(res);
        }
        scanner1.close();

        }
*/


    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("storesales"));
        // vv ValueFilterExample
        Filter filter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, // co ValueFilterExample-1-Filter Create filter, while specifying the comparison operator and comparator.
                new LongComparator(3));

        Scan scan = new Scan();
        scan.setFilter(filter); // co ValueFilterExample-2-SetFilter Set filter for the scan.
        ResultScanner scanner = table.getScanner(scan);
        // ^^ ValueFilterExample
        System.out.println("Results of scan:");
        // vv ValueFilterExample
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " + // co ValueFilterExample-3-Print1 Print out value to check that filter works.
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
            }
        }
        scanner.close();

        Get get = new Get(Bytes.toBytes("quantity"));
        get.setFilter(filter); // co ValueFilterExample-4-SetFilter2 Assign same filter to Get instance.
        Result result = table.get(get);
        // ^^ ValueFilterExample
        System.out.println("Result of get: ");
        // vv ValueFilterExample
        for (Cell cell : result.rawCells()) {
            System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                            cell.getValueLength()));
        }
        // ^^ ValueFilterExample
    }
}

/*
public class RegionObserver extends BaseRegionObserver {
    private static final byte[] ADMIN = Bytes.toBytes("admin");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("f1");
    private static final byte[] COLUMN = Bytes.toBytes("col1");
    private static final byte[] VALUE = Bytes.toBytes("You can not see Admin details");

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException{

        if (Bytes.equals(get.getRow(),ADMIN)) {
            Cell c = CellUtil.createCell(get.getRow(), COLUMN_FAMILY, COLUMN, System.currentTimeMillis(), (byte)4, VALUE);
            results.add(c);
            e.bypass();
        }

    }
}*/
