package com.hyr.cn.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*******************************************************************************
 * @date 2021-02-26 4:23 下午
 * @author: <a href=mailto:@>huangyr</a>
 * @Description: Kudu
 ******************************************************************************/
public class KuduDemo {

    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(KuduDemo.class);

    /*
        同步客户端，线程安全
     */
    private KuduClient kuduClient;
    /*
        异步客户端,只需要实例化一次
     */
    private AsyncKuduClient asyncKuduClient;

    private String tableName;

    @Before
    public void init() {
        // 创建kudu客户端
        tableName = "user";
        String kuduMaster = "server1:7051,server2:7051,server3:7051";
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMaster);
        AsyncKuduClient.AsyncKuduClientBuilder asyncKuduClientBuilder = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultAdminOperationTimeoutMs(10000);
        asyncKuduClientBuilder.defaultAdminOperationTimeoutMs(10000);
        asyncKuduClient = asyncKuduClientBuilder.build();
        kuduClient = kuduClientBuilder.build();
    }

    /**
     * 建表
     *
     * @throws KuduException
     */
    @Test
    public void testCreateTable() throws KuduException {
        if (!kuduClient.tableExists(tableName)) {
            ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("date", Type.INT32).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("money", Type.DOUBLE).build());
            Schema schema = new Schema(columnSchemas);

            CreateTableOptions options = new CreateTableOptions();
            List<String> hashKeys = new ArrayList<String>();
            //kudu表的分区字段是什么
            hashKeys.add("id");
            //按照id.hashcode % 分区数 = 分区号
            // 分区数
            int numBuckets = 8;
            options.addHashPartitions(hashKeys, numBuckets);
            // 设置副本数为1
            options.setNumReplicas(3);
            kuduClient.createTable(tableName, schema, options);
        } else {
            kuduClient.deleteTable("user");
        }
    }


    /**
     * 插入数据
     *
     * @throws KuduException
     */
    @Test
    public void insert() throws KuduException {
        final KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        for (int i = 0; i <= 1000000; i++) {
            // 打开表
            KuduTable userTable = kuduClient.openTable(tableName);
            Insert insert = userTable.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("id", i);
            row.addString("name", "wang" + i);
            row.addDouble("money", 100.342 + i);
            row.addInt("date", 20210225);
            kuduSession.apply(insert);
        }

        if (kuduSession.countPendingErrors() != 0) {
            // 是否错误溢出
            if (kuduSession.getPendingErrors().isOverflowed()) {
                logger.error("error collector had an overflow and had to discard row errors.");
            }
            RowError[] rowErrors = kuduSession.getPendingErrors().getRowErrors();
            for (RowError rowError : rowErrors) {
                logger.error("insert has error:{}", rowError);
            }
        }

        kuduSession.close();
    }

    /**
     * 查询
     *
     * @throws KuduException
     */
    @Test
    public void query() throws KuduException {
        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduScanner.KuduScannerBuilder kuduScannerBuilder = kuduClient
                .newScannerBuilder(kuduTable);
        Schema schema = kuduTable.getSchema();
        List<String> columns = Arrays.asList("id", "name", "money", "date");
        kuduScannerBuilder.setProjectedColumnNames(columns);
        // money >= 90000
        KuduPredicate moneyPredicate = KuduPredicate.newComparisonPredicate(schema.getColumn("money"), KuduPredicate.ComparisonOp.GREATER_EQUAL, 90000.0);
        KuduPredicate namePredicate = KuduPredicate.newComparisonPredicate(schema.getColumn("name"), KuduPredicate.ComparisonOp.EQUAL, "wang96556");

        KuduScanner kuduScanner = kuduScannerBuilder.addPredicate(namePredicate).addPredicate(moneyPredicate).build();
        while (kuduScanner.hasMoreRows()) {
            RowResultIterator rowResults = kuduScanner.nextRows();
            while (rowResults.hasNext()) {
                RowResult row = rowResults.next();
                logger.info("id={},name={},money={},date={}",
                        row.getInt("id"),
                        row.getString("name"),
                        row.getDouble("money"),
                        row.getInt("date"));
            }
        }
        kuduScanner.close();
    }


    /**
     * 更新数据
     *
     * @throws KuduException
     */
    @Test
    public void update() throws KuduException {
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        KuduTable kuduTable = kuduClient.openTable(tableName);

        //Update update = kuduTable.newUpdate();

        //id存在就修改，不存在就新增
        Upsert upsert = kuduTable.newUpsert();
        PartialRow row = upsert.getRow();
        row.addInt("id", 100000);
        row.addString("name", "huangyr");
        row.addDouble("money", 100.222);
        row.addInt("date", 20210226);
        RowError rowError = kuduSession.apply(upsert).getRowError();
        logger.info("get rowError:{}", rowError);
    }

    /**
     * 删除
     *
     * @throws KuduException
     */
    @Test
    public void delete() throws KuduException {
        KuduSession kuduSession = kuduClient.newSession();
        //设置手动刷新
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        KuduTable table = kuduClient.openTable(tableName);
        Delete delete = table.newDelete();
        delete.getRow().addInt("id", 96555);
        kuduSession.apply(delete);
        kuduSession.flush();
        kuduSession.close();
    }

    /**
     * 查询表的信息
     *
     * @throws KuduException
     */
    @Test
    public void queryTableInfo() throws KuduException {
        KuduSession kuduSession = kuduClient.newSession();
        //设置手动刷新
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        KuduTable table = kuduClient.openTable(tableName);
        Delete delete = table.newDelete();
        delete.getRow().addInt("id", 96555);
        kuduSession.apply(delete);
        kuduSession.flush();
        kuduSession.close();
    }

}
