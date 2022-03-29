package com.huawei.bigdata.spark.examples.datasources;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.spark.AvroSerdes;

import java.util.HashMap;
import java.util.Map;

public class AvroHBaseRecord {
    private String col0;
    private byte[] col1;

    AvroHBaseRecord(String col0, byte[] col1) {
        this.col1 = col1;
        this.col0 = col0;
    }

    public static AvroHBaseRecord apply(int i) {
        GenericData.Record user = new GenericData.Record(avroSchema);
        user.put("name", "name" + i);
        user.put("favorite_number", i);
        user.put("favorite_color", "color" + i);
        GenericData.Array favoriteArray =
                new GenericData.Array<String>(2, avroSchema.getField("favorite_array").schema());
        favoriteArray.add("number" + i);
        favoriteArray.add("number" + i);
        user.put("favorite_array", favoriteArray);
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("key1", i);
        map.put("key2", i + i);
        user.put("favorite_map", map);
        byte[] avroByte = AvroSerdes.serialize(user, avroSchema);
        return new AvroHBaseRecord("name" + i, avroByte);
    }

    public String getCol0() {
        return col0;
    }

    public void setCol0(String col0) {
        this.col0 = col0;
    }

    public byte[] getCol1() {
        return col1;
    }

    public void setCol1(byte[] col1) {
        this.col1 = col1;
    }

    public static String getSchemaString() {
        return schemaString;
    }

    public static Schema getAvroSchema() {
        return avroSchema;
    }

    static final String schemaString =
            "{\"namespace\": \"example.avro\","
                    + "\"type\": \"record\",      \"name\": \"User\","
                    + "\"fields\": ["
                    + "{\"name\": \"name\", \"type\": \"string\"},"
                    + "{\"name\": \"favorite_number\",  \"type\": [\"int\",\"null\"]}, "
                    + "{\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}, "
                    + "{\"name\": \"favorite_array\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},"
                    + "{\"name\": \"favorite_map\", \"type\": {\"type\": \"map\", \"values\": \"int\"}}]    }";
    static final Schema avroSchema = new Schema.Parser().parse(schemaString);
}
