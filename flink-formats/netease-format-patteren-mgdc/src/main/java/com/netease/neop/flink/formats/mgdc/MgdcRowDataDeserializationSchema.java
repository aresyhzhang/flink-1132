package com.netease.neop.flink.formats.mgdc;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MgdcRowDataDeserializationSchema
  implements DeserializationSchema<RowData> {
  private static final Logger log = LoggerFactory.getLogger( MgdcRowDataDeserializationSchema.class );


  private static final long serialVersionUID = 1L;


  private final Pattern pattern = Pattern.compile( "(?<pre>.*?)\\[(?<logtime>20.*?)]\\[(?<logtype>.*?)],(?<source>[\\s\\S]*)" );


  private final TypeInformation<RowData> producedTypeInfo;

  private final boolean ignoreParseErrors;

  private final List<RowType.RowField> rowTypeFields;


  public MgdcRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> producedTypeInfo, boolean ignoreParseErrors) {
    this.producedTypeInfo = producedTypeInfo;
    this.ignoreParseErrors = ignoreParseErrors;
    this.rowTypeFields = rowType.getFields();
  }


  @Override
  public RowData deserialize(byte[] message) throws IOException {
    throw new RuntimeException( "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead." );
  }


  @Override
  public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
    if (message == null || message.length == 0) {
      return;
    }
    try {
      String value = new String( message );
      Row row = mgdcPatternFlatMap( value );
      if (row.getArity() > 5) {
        GenericRowData rowData = new GenericRowData( this.rowTypeFields.size() );

        rowData.setField( 0, Long.valueOf( Long.parseLong( row.getField( 0 ).toString() ) ) );
        rowData.setField( 1, new BinaryStringData( row.getField( 1 ).toString() ) );
        rowData.setField( 2, new BinaryStringData( row.getField( 2 ).toString() ) );
        rowData.setField( 3, new BinaryStringData( row.getField( 3 ).toString() ) );
        rowData.setField( 4, new BinaryStringData( row.getField( 4 ).toString() ) );
        String jsonSource = row.getField( 5 ).toString();
        JSONObject jsonObject = JSONObject.parseObject( jsonSource );

        for (int i = 5; i < this.rowTypeFields.size(); i++) {
          RowType.RowField rowField = this.rowTypeFields.get( i );
          String fieldName = rowField.getName();
          if (jsonObject.containsKey( fieldName )) {
            LogicalType type = rowField.getType();
            LogicalTypeRoot typeRoot = type.getTypeRoot();
            switch (typeRoot) {
              case INTEGER:
                rowData.setField( i, jsonObject.getInteger( fieldName ) );
                break;
              case FLOAT:
                rowData.setField( i, jsonObject.getFloat( fieldName ) );
                break;
              case DOUBLE:
                rowData.setField( i, jsonObject.getDouble( fieldName ) );
                break;
              case BIGINT:
                rowData.setField( i, jsonObject.getLong( fieldName ) );
                break;
              case BOOLEAN:
                rowData.setField( i, jsonObject.getBoolean( fieldName ) );
                break;

              default:
                rowData.setField( i, new BinaryStringData( jsonObject
                        .getString( fieldName ) ) );
                break;
            }
          }
        }
        out.collect( rowData );
      }
    } catch (Throwable t) {
      String msg = String.format( "Failed to deserialize mgdc '%s',", new Object[]{new String( message )} );
      log.error( msg, t );
      if (!this.ignoreParseErrors) {
        throw new IOException( msg, t );
      }
    }
  }

  public Row mgdcPatternFlatMap(String value) {
    if (value.endsWith( "\n" )) {
      value = value.substring( 0, value.length() - 1 );
    }
    Matcher m = this.pattern.matcher( value );
    if (m.find()) {
      ZonedDateTime logDateTime;
      String logtime = m.group( "logtime" );

      if ((logtime.split( "\\s" )).length == 2) {
        if (logtime.length() > 19) {
          logtime = logtime.substring( 0, 19 );
        }


        logDateTime = LocalDateTime.parse( logtime, DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss" ) ).atZone( ZoneId.systemDefault() );

      } else {


        logDateTime = ZonedDateTime.parse( logtime, DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss Z" ) ).withZoneSameInstant( ZoneId.systemDefault() );
      }
      return Row.of( new Object[]{
              Long.valueOf( logDateTime.toInstant().getEpochSecond() ), m
              .group( "pre" ), logtime, m

              .group( "logtype" ), logDateTime
              .toLocalDateTime().format( DateTimeFormatter.BASIC_ISO_DATE ), m
              .group( "source" )} );
    }
    return null;
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