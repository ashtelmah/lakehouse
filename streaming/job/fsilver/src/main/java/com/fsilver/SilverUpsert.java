package com.fsilver;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Type;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class SilverUpsert extends KeyedProcessFunction<String, KafkaEvent, RowData> {

    private final String tableName;

    private transient Catalog catalog;
    private transient List<Types.NestedField> fields;
    private transient Map<String, Types.NestedField> fieldByName;

    public SilverUpsert(String tableName) {
        this.tableName = tableName.toLowerCase();
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        catalog = IcebergCatalogFactory.loadCatalog();

        Table t = catalog.loadTable(TableIdentifier.of("silver", tableName));
        fields = t.schema().columns();

        fieldByName = new HashMap<>();
        for (Types.NestedField f : fields) {
            fieldByName.put(f.name().toLowerCase(), f);
        }
    }

    @Override
    public void processElement(
            KafkaEvent event,
            KeyedProcessFunction<String, KafkaEvent, RowData>.Context ctx,
            Collector<RowData> out) throws Exception {

        switch (event.getOp()) {

            case INSERT:
            case SNAPSHOT: {
                GenericRowData row = buildFullRow(event.getAfter());
                row.setRowKind(RowKind.INSERT);
                out.collect(row);
                break;
            }

            case UPDATE: {
                GenericRowData before = buildFullRow(event.getBefore());
                before.setRowKind(RowKind.DELETE);
                out.collect(before);

                GenericRowData after = buildFullRow(event.getAfter());
                after.setRowKind(RowKind.INSERT);   // ← виправлено
                out.collect(after);
                break;
            }

            case DELETE: {
                GenericRowData row = buildFullRow(event.getBefore());
                row.setRowKind(RowKind.DELETE);
                out.collect(row);
                break;
            }
        }
    }

    private GenericRowData buildFullRow(Map<String, String> data) {

        GenericRowData row = new GenericRowData(fields.size());

        for (int i = 0; i < fields.size(); i++) {

            Types.NestedField f = fields.get(i);
            String col = f.name().toLowerCase();

            String val = (data != null) ? data.get(col) : null;

            if (col.startsWith("__$")) {
                row.setField(i, val == null ? null : BinaryStringData.fromString(val));
                continue;
            }

            Object converted = convertValue(f.type(), val);
            row.setField(i, converted);
        }

        return row;
    }

    private Object convertValue(Type type, String val) {

        if (val == null) {
            return null;
        }

        switch (type.typeId()) {

            case STRING:
                return BinaryStringData.fromString(val);

            case INTEGER:
                return Integer.parseInt(val);

            case LONG:
                return Long.parseLong(val);

            case BOOLEAN:
                return Boolean.parseBoolean(val);

            case FLOAT:
                return Float.parseFloat(val);

            case DOUBLE:
                return Double.parseDouble(val);

            case DATE:
                return Integer.parseInt(val);

            case TIMESTAMP:
                long millis = Long.parseLong(val);
                return TimestampData.fromEpochMillis(millis);

            case DECIMAL:
                Types.DecimalType dec = (Types.DecimalType) type;
                int precision = dec.precision();
                int scale = dec.scale();

                String cleaned = val.replaceAll("[\\p{Cntrl}]", "").trim();
                if (cleaned.isEmpty()) {
                    return null;
                }

                cleaned = cleaned.replace(',', '.');

                try {
                    BigDecimal bd = new BigDecimal(cleaned);
                    return DecimalData.fromBigDecimal(bd, precision, scale);
                } catch (NumberFormatException e) {
                    return null;
                }

            default:
                return BinaryStringData.fromString(val);
        }
    }
}
