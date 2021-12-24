package com.netease.neop.flink.formats.mgdc;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class MgdcOptions
{
  public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions.key("ignore-parse-errors")
    .booleanType()
    .defaultValue(Boolean.valueOf(false))
    .withDescription("Optional flag to skip fields and rows with parse errors instead of failing;\nfields are set to null in case of errors, false by default.");
}