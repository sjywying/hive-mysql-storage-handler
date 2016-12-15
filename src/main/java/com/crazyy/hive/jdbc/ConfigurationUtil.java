package com.crazyy.hive.jdbc;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;

import java.util.*;

public class ConfigurationUtil {
	public static final String LIST_COLUMNS 		= serdeConstants.LIST_COLUMNS;
	public static final String LIST_COLUMN_TYPES 	= serdeConstants.LIST_COLUMN_TYPES;


	public static final Set<String> ALL_PROPERTIES  = ImmutableSet.of(
			DBConfiguration.INPUT_CLASS_PROPERTY,
			DBConfiguration.URL_PROPERTY,
			DBConfiguration.USERNAME_PROPERTY,
			DBConfiguration.PASSWORD_PROPERTY,

			DBConfiguration.INPUT_TABLE_NAME_PROPERTY,
			DBConfiguration.INPUT_FIELD_NAMES_PROPERTY,

			DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY,
			DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY

//			DB_USERNAME, DB_PASSWORD, TABLE_NAME, COLUMN_MAPPING, COLUMN_IGNORED, COLUMN_SUM
	);

//	public final static String getDBUrl(Configuration conf){
//		return conf.get(DB_URL);
//	}
//
//	public final static String getDBUserName(Configuration conf) {
//		return conf.get(DB_USERNAME);
//	}
//
//	public final static String getDBPassword(Configuration conf) {
//		return conf.get(DB_PASSWORD);
//	}
//
//	public final static String getTableName(Configuration conf) {
//		return conf.get(TABLE_NAME);
//	}
//
//	public final static String getColumnMapping(Configuration conf) {
//		return conf.get(COLUMN_MAPPING);
//	}
//
//	public final static String getColumnIgnored(Configuration conf) {
//		return conf.get(COLUMN_IGNORED);
//	}
//
//	public final static String getColumnSum(Configuration conf) {
//		return conf.get(COLUMN_SUM);
//	}
	
	public static void copyMySQLProperties(Properties from,
			Map<String, String> to) {
		for (String key : ALL_PROPERTIES) {
			String value = from.getProperty(key);
			if (value != null) {
				to.put(key, value);
			}
		}
	}

	public static String[] getAllColumns(String columnMappingString) {
		if(Strings.isNullOrEmpty(columnMappingString)){
			return new String[0];
		}
		return columnMappingString.split(",");
	}
	
	public static List<String> getIngoredColumns(String columnString) {
		if(Strings.isNullOrEmpty(columnString)){
			return new ArrayList<String>();
		}
		return Arrays.asList(columnString.split(","));
	}
	
	public static List<String> getSumColumns(String columnString) {
		if(Strings.isNullOrEmpty(columnString)){
			return new ArrayList<String>();
		}
		return Arrays.asList(columnString.split(","));
	}
}
