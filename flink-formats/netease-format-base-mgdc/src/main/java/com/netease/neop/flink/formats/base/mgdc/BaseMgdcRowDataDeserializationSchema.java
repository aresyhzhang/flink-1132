package com.netease.neop.flink.formats.base.mgdc;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @ Author ：aresyhzhang @ Date ：Created in 13:59 2021/12/24 @ Description：采用base mgdc反序列化
 * 输出：rowtime,pid,logtime,logtype,source,date
 */
public class BaseMgdcRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    private static final Logger log =
            LoggerFactory.getLogger(BaseMgdcRowDataDeserializationSchema.class);

    private static final long serialVersionUID = 1L;

    private final TypeInformation<RowData> producedTypeInfo;

    private final boolean ignoreParseErrors;

    private final List<RowType.RowField> rowTypeFields;

    public BaseMgdcRowDataDeserializationSchema(
            RowType rowType, TypeInformation<RowData> producedTypeInfo, boolean ignoreParseErrors) {
        this.producedTypeInfo = producedTypeInfo;
        this.ignoreParseErrors = ignoreParseErrors;
        this.rowTypeFields = rowType.getFields();
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        if (message == null || message.length == 0) {
            return;
        }
        try {
            String value = new String(message);
            Row row = MgdcBaseLogFlatMapFunction.flatMap(value);
            assert row != null;
            if (row.getArity() > 5) {
                GenericRowData rowData = new GenericRowData(this.rowTypeFields.size());
                rowData.setField(0, Long.parseLong(row.getField(0).toString()));
                rowData.setField(1, new BinaryStringData(row.getField(1).toString()));
                rowData.setField(2, new BinaryStringData(row.getField(2).toString()));
                rowData.setField(3, new BinaryStringData(row.getField(3).toString()));
                // date
                rowData.setField(4, new BinaryStringData(row.getField(5).toString()));
                // source
                rowData.setField(5, new BinaryStringData(row.getField(4).toString()));
                out.collect(rowData);
            }
        } catch (Throwable t) {
            String msg = String.format("Failed to deserialize mgdc '%s',", new String(message));
            log.error(msg, t);
            if (!this.ignoreParseErrors) {
                throw new IOException(msg, t);
            }
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.producedTypeInfo;
    }
}
