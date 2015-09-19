import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable {
   private HTable hTable;

   public static void main(String[] args) throws IOException {
      // Instantiate Configuration class
      Configuration con = HBaseConfiguration.create();

      // Instaniate HBaseAdmin class
      HBaseAdmin admin = new HBaseAdmin(con);
      
      // Instantiate table descriptor class
      HTableDescriptor tableDescriptor = new
      HTableDescriptor(TableName.valueOf("powers"));

      // Add column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("personal"));
      tableDescriptor.addFamily(new HColumnDescriptor("professional"));

      // Execute the table through admin
      admin.createTable(tableDescriptor);
      System.out.println(" Table created ");

      // Instantiating HTable class
      hTable = new HTable(config, "powers");

      // Repeat these steps as many times as necessary
      addRow("row1", "superman", "strength", "clark", "100");
      addRow("row2", "batman", "money", "bruce", "50");
      addRow("row3", "wolverine", "healing", "logan", "75");

      // Close table

      // Instantiate the Scan class
      Scan scan = new Scan();
     
      // Scan the required columns
      scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));

      // Get the scan result
      ResultScanner scanner = table.getScanner(scan);

      // Read values from scan result
      // Print scan result
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
         System.out.println("Found row : " + result);
      }
 
      // Close the scanner
      scanner.close();

      // Htable closer
      hTable.close();
   }

   private static void addRow(String row, String hero, String power, String name, String xp) {
      Put p = new Put(Bytes.toBytes(row));

      p.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"),Bytes.toBytes(hero));

      p.add(Bytes.toBytes("personal"), Bytes.toBytes("power"),Bytes.toBytes(power));

      p.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes(name));

      p.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes(xp));

      hTable.put(p);
      System.out.println(row + " data inserted");
   }
}

