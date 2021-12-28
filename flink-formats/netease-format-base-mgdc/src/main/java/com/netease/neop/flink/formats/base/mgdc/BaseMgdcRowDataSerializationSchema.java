package com.netease.neop.flink.formats.base.mgdc;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.fastjson.JSONObject;
import com.netease.neop.flink.formats.base.mgdc.entity.BaseMgdcEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @ Author ：aresyhzhang @ Date ：Created in 13:59 2021/12/24 @ Description：序列化为mgdc格式
 * 输出：rowtime,pid,log_type,log_time,log_date,source 其中source是jsonObject，会自动封装用户字段为source
 */
public class BaseMgdcRowDataSerializationSchema implements SerializationSchema<RowData> {
    private static final Logger log =
            LoggerFactory.getLogger(BaseMgdcRowDataSerializationSchema.class);

    private final List<RowType.RowField> rowTypeFields;

    public BaseMgdcRowDataSerializationSchema(RowType rowType) {
        this.rowTypeFields = rowType.getFields();
    }

    @Override
    public byte[] serialize(RowData element) {
        try {
            BaseMgdcEntity mgdcEntity = new BaseMgdcEntity();
            JSONObject sourceObj = new JSONObject();
            for (int i = 0; i < this.rowTypeFields.size(); i++) {
                LogicalTypeRoot typeRoot;
                String fieldName = ((RowType.RowField) this.rowTypeFields.get(i)).getName();
                switch (fieldName) {
                    case "rowtime":
                        mgdcEntity.setRowtime(element.getLong(i));
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
                        typeRoot =
                                ((RowType.RowField) this.rowTypeFields.get(i))
                                        .getType()
                                        .getTypeRoot();
                        if (element.isNullAt(i)) break;
                        sourceObj.put(
                                fieldName, getJsonValue(typeRoot, element, Integer.valueOf(i)));
                        break;
                }
            }
            mgdcEntity.setSource(sourceObj.toJSONString());
            return mgdcEntity.toString().getBytes(StandardCharsets.UTF_8);
        } catch (Throwable t) {
            String msg = String.format("Could not serialize data '%s'.", element);
            log.error(msg);
            throw new RuntimeException(msg, t);
        }
    }

    private Object getJsonValue(LogicalTypeRoot typeRoot, RowData element, Integer index) {
        switch (typeRoot) {
            case INTEGER:
                return element.getInt(index);
            case FLOAT:
                return element.getFloat(index);
            case DOUBLE:
                return element.getDouble(index);
            case BIGINT:
                return element.getLong(index);
            case BOOLEAN:
                return element.getBoolean(index);
        }

        return element.getString(index).toString();
    }
}
