# hive-mysql-storage-handler
hive-mysql-storage-handler

后续会对此代码进行简化


注意点：
    1. 部署hive的节点和所有的DataNode节点需要授予数据库访问权限，由于此代码吞掉了部分异常信息造成排查困难。





参考
    https://github.com/qubole/Hive-JDBC-Storage-Handler
    hadoop-mapreduce-client-core.jar   org.apache.hadoop.mapreduce.lib.db


问题:
    mysql 类型为：tinyint(1)  mysql-connector-j 会将此类型映射为java中的boolean 所以hive需要用boolean进行接收，但hive中会存储false/true
    mysql 类型为：bigint unsigned  mysql-connector-j 会将此类型映射为java中的java.math.BigInteger会出现类型转换异常
            解决办法：
                修改hive-serde中JavaLongObjectInspector.java中41， 35行进行instanceof判断
                修改hive-service中Column.java  347行 同样进行instanceof判断