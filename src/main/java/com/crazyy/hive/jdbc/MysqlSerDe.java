package com.crazyy.hive.jdbc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.*;
import org.apache.hive.jdbc.JdbcColumn;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MysqlSerDe extends AbstractSerDe {

	public static final Log LOG = LogFactory.getLog(MysqlSerDe.class.getName());

	private DbRecordWritable cachedWritable;

	private int fieldCount;

	private StructObjectInspector objectInspector;
	private List<Object> deserializeCache;

	@Override
	public void initialize(Configuration configuration, Properties properties) throws SerDeException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("table properties : " + properties);
		}

		String columnNameProperty = properties.getProperty(ConfigurationUtil.LIST_COLUMNS);
		String columnTypeProperty = properties.getProperty(ConfigurationUtil.LIST_COLUMN_TYPES);

		// TODO 在定义hive表时自动检测mysql表结构进而初始化columnNameProperty、columnTypeProperty
//		if (columnNameProperty.length() == 0 && columnTypeProperty.length() == 0) {
//			JdbcSerDeHelper delegate = new JdbcSerDeHelper();
//			delegate.initialize(properties, configuration);
//			properties.setProperty(Constants.LIST_COLUMNS,
//					delegate.getColumnNames());
//			properties.setProperty(Constants.LIST_COLUMN_TYPES,
//					delegate.getColumnTypeNames());
//
//			columnNameProperty = properties.getProperty(Constants.LIST_COLUMNS);
//			columnTypeProperty = properties
//					.getProperty(Constants.LIST_COLUMN_TYPES);
//			LOG.info(">> " + columnNameProperty + " " + columnTypeProperty);
//		}

		List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
		String[] columnTypes = columnTypeProperty.split(":");
		assert (columnTypes.length == columnNames.size()) : "columnNames: "
				+ columnNames + ", columnTypes: "
				+ Arrays.toString(columnTypes);

		List<ObjectInspector> fieldOIs = null;
		try {
			int[] types = new int[columnTypes.length];
			for (int i = 0; i < columnTypes.length; i++) {
				types[i] = JdbcColumn.hiveTypeToSqlType(columnTypes[i]);
			}


			fieldOIs = new ArrayList<ObjectInspector>(columnTypes.length);
			for (int i = 0; i < types.length; i++) {
				ObjectInspector oi = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
						TypeInfoFactory.getPrimitiveTypeInfo(columnTypes[i]));
				fieldOIs.add(oi);
			}

			this.cachedWritable = new DbRecordWritable(types);
			this.fieldCount = types.length;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		this.objectInspector = ObjectInspectorFactory
				.getStandardStructObjectInspector(columnNames, fieldOIs);
		this.deserializeCache = new ArrayList<Object>(columnTypes.length);
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return DbRecordWritable.class;
	}

	@Override
	public Writable serialize(Object row, ObjectInspector objectInspector) throws SerDeException {
		final StructObjectInspector structInspector = (StructObjectInspector) objectInspector;
		final List<? extends StructField> fields = structInspector.getAllStructFieldRefs();
		if (fields.size() != fieldCount) {
			throw new SerDeException(String.format(
					"Required %d columns, received %d.", fieldCount,
					fields.size()));
		}

		cachedWritable.clear();

		for (int i = 0; i < fieldCount; i++) {
			StructField structField = fields.get(i);
			if (structField != null) {
				Object field = structInspector.getStructFieldData(row, structField);
				ObjectInspector fieldOI = structField.getFieldObjectInspector();
				Object javaObject = deparseObject(field, fieldOI);
				cachedWritable.set(i, javaObject);
			}
		}

		return cachedWritable;
	}

	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

	@Override
	public Object deserialize(Writable writable) throws SerDeException {
		if (!(writable instanceof DbRecordWritable)) {
			throw new SerDeException("Expected DbTupleWritable, received " + writable.getClass().getName());
		}

		DbRecordWritable tuple = (DbRecordWritable) writable;
		deserializeCache.clear();

		for (int i = 0; i < fieldCount; i++) {
			Object o = tuple.get(i);
			deserializeCache.add(o);
		}

		return deserializeCache;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return objectInspector;
	}

	public static Object deparseObject(Object field, ObjectInspector fieldOI)
			throws SerDeException {
		switch (fieldOI.getCategory()) {
			case PRIMITIVE: {
				PrimitiveObjectInspector oi = (PrimitiveObjectInspector) fieldOI;
				return oi.getPrimitiveJavaObject(field);
			}
			case LIST: {
				ListObjectInspector listOI = (ListObjectInspector) fieldOI;
				List<?> elements = listOI.getList(field);
				List<Object> list = new ArrayList<Object>(elements.size());
				ObjectInspector elemOI = listOI.getListElementObjectInspector();
				for (Object elem : elements) {
					Object o = deparseObject(elem, elemOI);
					list.add(o);
				}
				return list;
			}
			default:
				throw new SerDeException("Unexpected fieldOI: " + fieldOI);
		}
	}
}
