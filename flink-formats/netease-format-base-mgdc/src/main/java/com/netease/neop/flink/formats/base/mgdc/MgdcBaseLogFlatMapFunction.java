package com.netease.neop.flink.formats.base.mgdc;

import org.apache.flink.types.Row;

import com.netease.neop.common.opd.BaseLog;
import com.netease.neop.common.opd.OpdUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/** mgdc 日志解析 */
@Slf4j
public class MgdcBaseLogFlatMapFunction {
    public static final ZoneId UTC_8_ZONE_ID = ZoneId.of("UTC+8");
    public static final DateTimeFormatter dateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static Row flatMap(String value) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        if (value.endsWith("\n")) {
            value = value.substring(0, value.length() - 1);
        }
        BaseLog baseLog = BaseLog.buildFromStr(value);
        if (baseLog == null) {
            log.warn("can't parse log:{}", value);
            return null;
        }
        String date = null;
        String logtime = baseLog.getLogtime();
        ZonedDateTime zonedDateTime = baseLog.getZonedDateTime();
        if (zonedDateTime != null) {
            date =
                    zonedDateTime
                            .withZoneSameInstant(UTC_8_ZONE_ID)
                            .toLocalDate()
                            .format(DateTimeFormatter.BASIC_ISO_DATE);
        } else {
            if (StringUtils.isNotEmpty(logtime)) {
                for (DateTimeFormatter formatter : OpdUtils.getDateTimeFormatter()) {
                    try {
                        zonedDateTime = ZonedDateTime.parse(logtime, formatter);
                        date =
                                zonedDateTime
                                        .withZoneSameInstant(UTC_8_ZONE_ID)
                                        .toLocalDate()
                                        .format(DateTimeFormatter.BASIC_ISO_DATE);
                        break;
                    } catch (Exception ignored) {
                    }
                }
            }
        }
        if (StringUtils.isEmpty(date)) {
            zonedDateTime = ZonedDateTime.now();
            date =
                    zonedDateTime
                            .withZoneSameInstant(UTC_8_ZONE_ID)
                            .toLocalDate()
                            .format(DateTimeFormatter.BASIC_ISO_DATE);
        }
        if (StringUtils.isEmpty(logtime)) {
            logtime =
                    zonedDateTime
                            .withZoneSameInstant(UTC_8_ZONE_ID)
                            .toLocalDateTime()
                            .format(dateTimeFormatter);
        }

        return Row.of(
                zonedDateTime.withZoneSameInstant(UTC_8_ZONE_ID).toInstant().getEpochSecond(),
                baseLog.getPid(),
                logtime,
                baseLog.getLogtype(),
                baseLog.getSource(),
                date);
    }
}
