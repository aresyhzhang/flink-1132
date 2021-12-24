package com.netease.neop.flink.formats.base.mgdc;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class MgdcFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {
    public static final String IDENTIFIER = "base-mgdc";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, final ReadableConfig formatOptions) {
        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
                RowType rowType = (RowType) producedDataType.getLogicalType();

                TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation( producedDataType );
                Boolean isIgnore = formatOptions.getOptional( MgdcOptions.IGNORE_PARSE_ERRORS ).orElse( Boolean.valueOf( true ) );
                return new MgdcRowDataDeserializationSchema( rowType, rowDataTypeInfo, isIgnore.booleanValue() );
            }


            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }


    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(DynamicTableSink.Context context, DataType physicalDataType) {
                RowType rowType = (RowType) physicalDataType.getLogicalType();
                return new MgdcRowDataSerializationSchema( rowType );
            }


            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }


    /**
     * @return
     */
    @Override
    public String factoryIdentifier() {
        return "base-mgdc";
    }


    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }


    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add( MgdcOptions.IGNORE_PARSE_ERRORS );
        return options;
    }
}