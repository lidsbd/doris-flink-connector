// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.tools.cdc;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;

import org.apache.doris.flink.tools.cdc.mysql.MysqlDatabaseSync;
import org.apache.doris.flink.tools.cdc.oracle.OracleDatabaseSync;
import org.apache.doris.flink.tools.cdc.postgres.PostgresDatabaseSync;
import org.apache.doris.flink.tools.cdc.sqlserver.SqlServerDatabaseSync;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** cdc sync tools. */
public class CdcTools {
    private static final String MYSQL_SYNC_DATABASE = "mysql-sync-database";
    private static final String ORACLE_SYNC_DATABASE = "oracle-sync-database";
    private static final String POSTGRES_SYNC_DATABASE = "postgres-sync-database";
    private static final String SQLSERVER_SYNC_DATABASE = "sqlserver-sync-database";
    private static final List<String> EMPTY_KEYS = Arrays.asList("password");

    /**
     * 使用说明：
     * bin/flink run \
     *     -c org.apache.doris.flink.tools.cdc.CdcTools \
     *     lib/flink-doris-connector-1.16-1.4.0-SNAPSHOT.jar \
     *     <mysql-sync-database|oracle-sync-database|postgres-sync-database|sqlserver-sync-database> \
     *     --database <doris-database-name> \
     *     [--job-name <flink-job-name>] \
     *     [--table-prefix <doris-table-prefix>] \
     *     [--table-suffix <doris-table-suffix>] \
     *     [--including-tables <mysql-table-name|name-regular-expr>] \
     *     [--excluding-tables <mysql-table-name|name-regular-expr>] \
     *     --mysql-conf <mysql-cdc-source-conf> [--mysql-conf <mysql-cdc-source-conf> ...] \
     *     --oracle-conf <oracle-cdc-source-conf> [--oracle-conf <oracle-cdc-source-conf> ...] \
     *     --sink-conf <doris-sink-conf> [--table-conf <doris-sink-conf> ...] \
     *     [--table-conf <doris-table-conf> [--table-conf <doris-table-conf> ...]]
     *
     * 使用案例
     *       <FLINK_HOME>bin/flink run \
     *     -Dexecution.checkpointing.interval=10s \
     *     -Dparallelism.default=1 \
     *     -c org.apache.doris.flink.tools.cdc.CdcTools \
     *     lib/flink-doris-connector-1.16-1.4.0-SNAPSHOT.jar \
     *     mysql-sync-database \
     *     --database test_db \
     *     --mysql-conf hostname=127.0.0.1 \
     *     --mysql-conf port=3306 \
     *     --mysql-conf username=root \
     *     --mysql-conf password=123456 \
     *     --mysql-conf database-name=mysql_db \
     *     --including-tables "tbl1|test.*" \
     *     --sink-conf fenodes=127.0.0.1:8030 \
     *     --sink-conf username=root \
     *     --sink-conf password=123456 \
     *     --sink-conf jdbc-url=jdbc:mysql://127.0.0.1:9030 \
     *     --sink-conf sink.label-prefix=label \
     *     --table-conf replication_num=1
     *  @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String operation = args[0].toLowerCase();
        String[] opArgs = Arrays.copyOfRange(args, 1, args.length);
        System.out.println();
        switch (operation) {
            case MYSQL_SYNC_DATABASE:
                createMySQLSyncDatabase(opArgs);
                break;
            case ORACLE_SYNC_DATABASE:
                createOracleSyncDatabase(opArgs);
                break;
            case POSTGRES_SYNC_DATABASE:
                createPostgresSyncDatabase(opArgs);
                break;
            case SQLSERVER_SYNC_DATABASE:
                createSqlServerSyncDatabase(opArgs);
                break;
            default:
                System.out.println("Unknown operation " + operation);
                System.exit(1);
        }
    }

    private static void createMySQLSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        Map<String, String> mysqlMap = getConfigMap(params, "mysql-conf");
        //取出mysql相关配置信息
        Configuration mysqlConfig = Configuration.fromMap(mysqlMap);
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        syncDatabase(params, databaseSync, mysqlConfig, "MySQL");
    }

    private static void createOracleSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        Map<String, String> oracleMap = getConfigMap(params, "oracle-conf");
        Configuration oracleConfig = Configuration.fromMap(oracleMap);
        DatabaseSync databaseSync = new OracleDatabaseSync();
        syncDatabase(params, databaseSync, oracleConfig, "Oracle");
    }

    private static void createPostgresSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        Map<String, String> postgresMap = getConfigMap(params, "postgres-conf");
        Configuration postgresConfig = Configuration.fromMap(postgresMap);
        DatabaseSync databaseSync = new PostgresDatabaseSync();
        syncDatabase(params, databaseSync, postgresConfig, "Postgres");
    }

    private static void createSqlServerSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        Map<String, String> postgresMap = getConfigMap(params, "sqlserver-conf");
        Configuration postgresConfig = Configuration.fromMap(postgresMap);
        DatabaseSync databaseSync = new SqlServerDatabaseSync();
        syncDatabase(params, databaseSync, postgresConfig, "SqlServer");
    }

    /**
     * private类型：只能在本类中调用，子类也会继承但是不能直接调用
     *
     * @param params ：接收到启动任务的配置参数
     * @param databaseSync ：创建的MysqlDatabaseSync稍后研究做什么用
     * @param config ：源数据库的配置，如果是mysql就是mysqlConfig
     * @param type: 元数据是mysql就是传MYSQL
     * @throws Exception
     */
    private static void syncDatabase(
            MultipleParameterTool params,
            DatabaseSync databaseSync,
            Configuration config,
            String type)
            throws Exception {
        // TODO 初始化配置信息完成
        String jobName = params.get("job-name");
        String database = params.get("database");
        String tablePrefix = params.get("table-prefix");
        String tableSuffix = params.get("table-suffix");
        String includingTables = params.get("including-tables");
        String excludingTables = params.get("excluding-tables");
        String multiToOneOrigin = params.get("multi-to-one-origin");
        String multiToOneTarget = params.get("multi-to-one-target");
        boolean createTableOnly = params.has("create-table-only");
        boolean ignoreDefaultValue = params.has("ignore-default-value");
        boolean useNewSchemaChange = params.has("use-new-schema-change");
        boolean singleSink = params.has("single-sink");

        Map<String, String> sinkMap = getConfigMap(params, "sink-conf");
        Map<String, String> tableMap = getConfigMap(params, "table-conf");
        Configuration sinkConfig = Configuration.fromMap(sinkMap);

        //TODO 构建Flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        databaseSync
                .setEnv(env)
                .setDatabase(database)
                .setConfig(config)
                .setTablePrefix(tablePrefix)
                .setTableSuffix(tableSuffix)
                .setIncludingTables(includingTables)
                .setExcludingTables(excludingTables)
                .setMultiToOneOrigin(multiToOneOrigin)
                .setMultiToOneTarget(multiToOneTarget)
                .setIgnoreDefaultValue(ignoreDefaultValue)
                .setSinkConfig(sinkConfig)
                .setTableConfig(tableMap)
                .setCreateTableOnly(createTableOnly)
                .setNewSchemaChange(useNewSchemaChange)
                .setSingleSink(singleSink)
                .create();
        //TODO 执行整个采集逻辑
        databaseSync.build();
        if (StringUtils.isNullOrWhitespaceOnly(jobName)) {
            jobName =
                    String.format(
                            "%s-Doris Sync Database: %s",
                            type, config.getString("database-name", "db"));
        }
        env.execute(jobName);
    }

    private static Map<String, String> getConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            return null;
        }

        Map<String, String> map = new HashMap<>();
        for (String param : params.getMultiParameter(key)) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2) {
                map.put(kv[0], kv[1]);
                continue;
            } else if (kv.length == 1 && EMPTY_KEYS.contains(kv[0])) {
                map.put(kv[0], "");
                continue;
            }

            System.err.println("Invalid " + key + " " + param + ".\n");
            return null;
        }
        return map;
    }
}
