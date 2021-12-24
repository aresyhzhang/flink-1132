package com.netease.neop.flink.formats.mgdc;

import com.alibaba.fastjson.JSONObject;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.netease.neop.flink.formats.mgdc.entity.MgdcEntity;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MgdcRowDataSerializationSchema
        implements SerializationSchema<RowData>
{
  private static final Logger log = LoggerFactory.getLogger(MgdcRowDataSerializationSchema.class);

  private final List<RowType.RowField> rowTypeFields;


  public MgdcRowDataSerializationSchema(RowType rowType) {
    this.rowTypeFields = rowType.getFields();
  }


  @Override
  public byte[] serialize(RowData element) {
    try {
      MgdcEntity mgdcEntity = new MgdcEntity();
      JSONObject sourceObj = new JSONObject();
      for (int i = 0; i < this.rowTypeFields.size(); i++) {
        LogicalTypeRoot typeRoot; String fieldName = ((RowType.RowField)this.rowTypeFields.get(i)).getName();
        switch (fieldName) {
          case "rowtime":
            mgdcEntity.setRowtime( element.getLong( i ) );
            break;
          case "pid":
            mgdcEntity.setPid(element.getString(i).toString());
            break;
          case "log_type":
            mgdcEntity.setLogType(element.getString(i).toString());
            break;
          case "log_time":
            mgdcEntity.setLogTime(element.getString(i).toString());
            break;
          case "log_date":
            mgdcEntity.setLogDate(element.getString(i).toString());
            break;
          default:
            typeRoot = ((RowType.RowField)this.rowTypeFields.get(i)).getType().getTypeRoot();
            if (element.isNullAt(i))
              break;  sourceObj.put(fieldName, getJsonValue(typeRoot, element, Integer.valueOf(i))); break;
        }
      }
      mgdcEntity.setSource(sourceObj.toJSONString());
      return mgdcEntity.toString().getBytes(StandardCharsets.UTF_8);
    } catch (Throwable t) {
      String msg = String.format("Could not serialize data '%s'.", element );
      log.error(msg);
      throw new RuntimeException(msg, t);
    }
  }

  private Object getJsonValue(LogicalTypeRoot typeRoot, RowData element, Integer index) {
    switch (typeRoot) {
      case INTEGER:
        return element.getInt( index );
      case FLOAT:
        return element.getFloat( index );
      case DOUBLE:
        return element.getDouble( index );
      case BIGINT:
        return element.getLong( index );
      case BOOLEAN:
        return element.getBoolean( index );
    }

    return element.getString( index ).toString();
  }
}