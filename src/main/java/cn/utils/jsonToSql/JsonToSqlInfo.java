package cn.utils.jsonToSql;

import lombok.Data;

@Data
public class JsonToSqlInfo {
    private String schemaName;
    private String tableName;
    private String filePath;

    public String getAllTableName() {
        if (tableName != null) {
            if (schemaName != null) {
                return String.format("%s.%s", schemaName, tableName);
            } else {
                return tableName;
            }
        } else {
            return "tb_simple";
        }

    }
}
