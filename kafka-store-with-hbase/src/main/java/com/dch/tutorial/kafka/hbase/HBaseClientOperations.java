package com.dch.tutorial.kafka.hbase;

import com.dch.tutorial.kafka.config.HBaseConfig;
import com.dch.tutorial.kafka.model.User;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Basic HBase client operations.
 *
 * @author David.Christianto
 */
public class HBaseClientOperations {

    public static final String TABLE_NAME = "user";
    public static final byte[] FAMILY_NAME = Bytes.toBytes("name");
    public static final byte[] FAMILY_CONTACT_INFO = Bytes.toBytes("contactInfo");
    public static final byte[] QUALIFIER_FIRST = Bytes.toBytes("first");
    public static final byte[] QUALIFIER_LAST = Bytes.toBytes("last");
    public static final byte[] QUALIFIER_EMAIL = Bytes.toBytes("email");

    /**
     * Method used to check if table is exist in HBase or not.
     *
     * @param tableName {@link TableName}
     * @return true if table exists and vice versa.
     * @throws IOException Error occurred when check existing table in HBase.
     */
    public boolean isTableExists(TableName tableName) throws IOException {
        return HBaseConfig.getConnection().getAdmin().tableExists(tableName);
    }

    /**
     * Method used to initialize HBase table.
     *
     * @param clientOperations Basic HBase client operation.
     * @throws IOException If error occurred when HBase client failed to initialize table.
     */
    public void initHBase(HBaseClientOperations clientOperations) throws IOException {
        TableName tableName = TableName.valueOf(HBaseClientOperations.TABLE_NAME);
        if (!isTableExists(tableName)) {
            createTable();
        }
    }

    /**
     * Create the table with two column families.
     *
     * @throws IOException Error occurred when create the table.
     */
    public void createTable() throws IOException {
        Connection connection = HBaseConfig.getConnection();
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        tableDescriptor.addFamily(new HColumnDescriptor("name"));
        tableDescriptor.addFamily(new HColumnDescriptor("contactInfo"));
        connection.getAdmin().createTable(tableDescriptor);
    }

    /**
     * Add each user to the table. <br/>
     * Use the `name` column family for the name. <br/>
     * Use the `contactInfo` column family for the email
     *
     * @param tableName {@link TableName}
     * @throws IOException
     */
    public void put(TableName tableName, List<User> users) throws IOException {
        Connection connection = HBaseConfig.getConnection();
        Table table = connection.getTable(tableName);
        users.forEach(user -> {
            try {
                Put put = new Put(Bytes.toBytes(user.getId()));
                put.addImmutable(FAMILY_NAME, QUALIFIER_FIRST, Bytes.toBytes(user.getFirstName()));
                put.addImmutable(FAMILY_NAME, QUALIFIER_LAST, Bytes.toBytes(user.getLastName()));
                put.addImmutable(FAMILY_CONTACT_INFO, QUALIFIER_EMAIL, Bytes.toBytes(user.getEmail()));
                table.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Method used to scan all data in selected table.
     *
     * @param tableName {@link TableName}
     * @return List of user.
     * @throws IOException Error occurred when scanning data in HBase table.
     */
    public List<User> scan(TableName tableName) throws IOException {
        List<User> users = new ArrayList<>();
        Connection connection = HBaseConfig.getConnection();
        Table table = connection.getTable(tableName);
        try (ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result result : scanner) {
                User user = new User();
                user.setId(Bytes.toLong(result.getRow()));
                user.setFirstName(Bytes.toString(result.getValue(FAMILY_NAME, QUALIFIER_FIRST)));
                user.setLastName(Bytes.toString(result.getValue(FAMILY_NAME, QUALIFIER_LAST)));
                user.setEmail(Bytes.toString(result.getValue(FAMILY_CONTACT_INFO, QUALIFIER_EMAIL)));
                users.add(user);
            }
        }
        return users;
    }

    /**
     * Method used to delete the specified table.
     *
     * @param tableName {@link TableName} Table name to delete.
     * @throws IOException
     */
    public void deleteTable(TableName tableName) throws IOException {
        Connection connection = HBaseConfig.getConnection();
        if (connection.getAdmin().tableExists(tableName)) {
            connection.getAdmin().disableTable(tableName);
            connection.getAdmin().deleteTable(tableName);
        }
    }
}
