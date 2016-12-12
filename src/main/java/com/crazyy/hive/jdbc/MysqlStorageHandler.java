package com.crazyy.hive.jdbc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.jdbc.storagehandler.Constants;
import org.apache.hadoop.hive.jdbc.storagehandler.DbRecordWritable;
import org.apache.hadoop.hive.jdbc.storagehandler.JdbcSerDe;
import org.apache.hadoop.hive.jdbc.storagehandler.JdbcSerDeHelper;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;

import java.util.*;

public class MysqlStorageHandler implements HiveStorageHandler, HiveStoragePredicateHandler {

    private static final Log LOG = LogFactory.getLog(MysqlStorageHandler.class);

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return null;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return null;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return MysqlSerDe.class;
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

    }

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {

    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new HiveMetaHook() {
            @Override
            public void preCreateTable(Table table) throws MetaException {
                if (!MetaStoreUtils.isExternalTable(table)) {
                    throw new MetaException("Table must be external.");
                }
            }

            @Override
            public void rollbackCreateTable(Table table) throws MetaException {

            }

            @Override
            public void commitCreateTable(Table table) throws MetaException {

            }

            @Override
            public void preDropTable(Table table) throws MetaException {

            }

            @Override
            public void rollbackDropTable(Table table) throws MetaException {

            }

            @Override
            public void commitDropTable(Table table, boolean deleteData) throws MetaException {

            }
        };
    }

    /****************************  @see Configuration  **********************/

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

    /********** @see org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler *****************/
    @Override
    public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc predicate) {
        return null;
    }


}
