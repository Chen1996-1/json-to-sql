package cn.utils.jsonToSql;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class JsonToSqlServiceImpl implements JsonToSqlService{
    @Override
    public String[] JsonToSql(JsonToSqlInfo jsonToSqlInfo) {
        String tableName = jsonToSqlInfo.getAllTableName();
        String jsonFilePath = jsonToSqlInfo.getFilePath();
        String jsonStr = ReadFileToJson(jsonFilePath);
        if (!Objects.equals(jsonStr, "")) {
            Object jsonObj = JSONObject.parse(jsonStr);
            Integer dataCount = 0;
            ArrayList<Integer> dataCountList = new ArrayList<>();
            dataCountList.add(dataCount);
            Map<String, ArrayList<Integer>> tableNameCountMap = new HashMap<>();
            tableNameCountMap.put(tableName,dataCountList);
            CreateTableFromJson(tableNameCountMap,tableName, jsonObj, null);
        }

        return null;
    }

    private String  ReadFileToJson(String jsonFilePath) {
        String jsonStr = "";
        try {
            File jsonFile = new File(jsonFilePath);
            FileReader fileReader = new FileReader(jsonFile);
            Reader reader = new InputStreamReader(new FileInputStream(jsonFile), StandardCharsets.UTF_8);
            int ch = 0;
            StringBuilder sb = new StringBuilder();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
        } catch (IOException e) {
            e.printStackTrace();

        }
        return jsonStr;
    }

    private Integer getTableNameCountMapSum(ArrayList<Integer> arrayList) {
        int sum = 0;
        for (Integer d :
                arrayList) {
            sum = sum + d;
        }
        return sum;
    }

    /**
     * @param tableNameCountMap 创建表表名,及 数据id 初始数量
     * @param SourceObj jsonArray 对象
     * 1. 拿到 jsonArray 对象
     * 2. 解析全部结构，看看要建多少张表， 如果有多张表，那么就要建一张视图
     *                  2.1. 假设只建一张表，
     *                      2.1.1 获取所有字段，获取字段对应的类型，其中类型不应包含  jsonObject和jsonArray中的元素为jsonObject 的情况
     *                      2.1.2 integer、bigDecimal 统一搞成 decimal(12,8), jsonArray, 搞成 Array, 支持 指定 字段名存储为指定格式而不统一转换成关联表
     *                            String varchar,
     *                  2.2  假设不能只建一张表，那么就是要额外创建一个视图，多张表，从左往右建，视图最后创建，这个字段结构：
     *                      2.2.1 例如：最左边的表tb_simple,字段js，如果 类型为 jsonArray(jsonObject)或者 jsonObject,那就拿到这个jsonObject 对应的key,字段改名为 pk_simple_b_js_id
     *                              建立tb_simple_branch_js, tb_simple_branch_js_branch_log, 拥有自增id
     *                              tb_simple_branch_ts
     *
     */
    private void CreateTableFromJson(Map<String, ArrayList<Integer>> tableNameCountMap,String tableName, Object SourceObj, Map<String,String> customFieldTypeMap) {
        Integer initDataCount = getTableNameCountMapSum(tableNameCountMap.get(tableName));
        int counter = 0;

        ArrayList<String> fieldArray = initFieldList(tableName);
        ArrayList<String> fieldTypeArray = initFiledTypeList(tableName);


        ArrayList<String[]> dataValuesList = new ArrayList<>();

        JSONArray jsonArray = new JSONArray();

        if (SourceObj instanceof JSONArray) {
            jsonArray = (JSONArray) SourceObj;
        }else if (SourceObj instanceof JSONObject){
            jsonArray = jsonArray.fluentAdd(SourceObj);
        }

        /*创建表*/
        for (int i = 0, jsonArraySize = jsonArray.size(); i < jsonArraySize; i++) {
            Object obj = jsonArray.get(i);

            if (!(obj instanceof JSONObject)) {
                break;
            } else {
                JSONObject jsonObject = (JSONObject) obj;
                Set<String> keySet = jsonObject.keySet();
                for (String k :
                        keySet) {
                    if (!(fieldArray.contains(k))) {
                        String type = getTypeString(customFieldTypeMap, jsonObject, k);
                        if (Objects.equals(type, "int8") || Objects.equals(type, "int8[]")) {
                            String branchTableName = String.format("%s_branch_%s", tableName, k);
                            Object branchJsonArray = jsonObject.get(k);
                            k = String.format("pk_%s_id", k);
                            if(!tableNameCountMap.containsKey(branchTableName)){
                                ArrayList<Integer> branchCount = new ArrayList<>();
                                branchCount.add(counter);
                                tableNameCountMap.put(branchTableName, branchCount);
                            }
                            CreateTableFromJson(tableNameCountMap,branchTableName, branchJsonArray, customFieldTypeMap);
                        }
                        if (!fieldArray.contains(k)) {
                            fieldTypeArray.add(type);
                            fieldArray.add(k);
                        }

                    }

                }
            }
        }
        String createTableSql = getCreateTableSql(tableName, fieldArray, fieldTypeArray);
//        System.out.println(createTableSql);
//        System.out.println(dataCount);
        /* 插入数据*/

        for (int i=0; i<jsonArray.size(); i++) {
            // TODO: 2022/6/5  需要把 这个数量存起来，跟表名对应，下次接着写入

            Object obj = jsonArray.get(i);
            String[] dataValues = new String[fieldArray.size()];
            if (!(obj instanceof JSONObject)) {
                break;
            } else {
                JSONObject jsonObject = (JSONObject) obj;

                for (int j = 0; j < fieldArray.size(); j++) {
                    Object data = jsonObject.get(fieldArray.get(j));
                    String dataValue;

                    if ("text[]".equals(fieldTypeArray.get(j)) || "float4[]".equals(fieldTypeArray.get(j))) {
                        dataValue = String.format("array%s", data).replaceAll("\"", "'");
                    } 
                    else if ("int8[]".equals(fieldTypeArray.get(j))) {
                        // TODO: 2022/6/5  找到 对应的表，然后看他的条数，
                        String arrId = fieldArray.get(j);
                        dataValue = getArrDataValueByFieldId(tableNameCountMap, arrId, counter);


                    }
                    else if (fieldTypeArray.get(j).startsWith("int8")) {
                        dataValue = String.format("'%s'", initDataCount+counter);
                    } else {
                        if (data == null) {
                            dataValue = "null";
                        } else {
                            dataValue = String.format("'%s'", data);
                        }
                    }

                    dataValues[j] = dataValue;
                }

            }
            dataValuesList.add(dataValues);
            counter++;
        }

        ArrayList<String> insertDataSql = getInsertDataSql(tableName, dataValuesList, fieldArray);
//        insertDataSql.forEach(System.out::println);
        ArrayList<Integer> a = tableNameCountMap.get(tableName);
        a.add(counter);
        tableNameCountMap.replace(tableName, a);
        /*创建视图*/

        /*select  a.*, json_build_object('type', d.type, 'coordinates',d.coordinates)::text as dgeom, json_build_object('type', m.type, 'coordinates',m.coordinates)::text as mgeom	   from         psplat_v2.tb_simple a
           left join psplat_v2.tb_simple_branch_dgeom d on a.pk_dgeom_id = d.pk_tb_simple_branch_dgeom_id
           left join psplat_v2.tb_simple_branch_mgeom m on a.pk_mgeom_id = m.pk_tb_simple_branch_mgeom_id
*/
    }

    private String getArrDataValueByFieldId(Map<String, ArrayList<Integer>> tableNameCountMap, String arrId, Integer initDataCount) {
        String targetName="";
        String dataValue = "";
        String fieldId = arrId.replace("pk_", "").replace("_id", "");
        Set<String> tNSet = tableNameCountMap.keySet();
        for (String tn :
                tNSet) {
            if (tn.endsWith(fieldId)) {
                targetName = tn;
            }
        }
        if (targetName != "") {
           Integer end =  tableNameCountMap.get(targetName).get(initDataCount+1);
            Integer start = tableNameCountMap.get(targetName).get(initDataCount);
            if (end != null && start != null && end >= start) {
                String res = "";
                for (int i = start; i < end; i++) {
                    if (i == start) {
                        res = String.format("%s%s", res, i);
                    } else {
                        res = String.format("%s,%s", res, i);
                    }
                }
                dataValue = String.format("array[%s]", res);
            }

        }
        if (Objects.equals(dataValue, "")) {
            return null;
        }
        return dataValue;
    }

    private ArrayList<String> getInsertDataSql(String tableName, ArrayList<String[]> dataValuesList, ArrayList<String> fieldArray) {
        ArrayList<String> resInsertSql = new ArrayList<>();

        String insertDataSql = String.format("insert into %s (", tableName);
        String fieldSql = "";
        String dataSql = "";
        for (int i = 0; i < fieldArray.size(); i++) {
            if (i == fieldArray.size() - 1) {
                fieldSql = fieldSql + String.format("%s) values ", fieldArray.get(i));
            } else {
                fieldSql =  fieldSql + String.format("%s,", fieldArray.get(i));
            }
        }
        insertDataSql = insertDataSql + fieldSql;


        for (int i = 0; i < dataValuesList.size(); i++) {
            String[] dataValues = dataValuesList.get(i);
            dataSql = dataSql + "(";
            for (int j = 0; j <dataValues.length ; j++) {
                if (j == dataValues.length - 1) {
                    dataSql = dataSql + dataValues[j] + ")";
                } else {
                    dataSql = dataSql + dataValues[j] + ",";
                }
            }
            if (i == dataValuesList.size() - 1 || (i>1 &&i % 100 == 0)) {
                if (tableName.contains("branch")) {
                    dataSql = dataSql + String.format(" on conflict(pk_%s_id) do nothing;", tableName);
                } else {
                    dataSql = dataSql + String.format(" on conflict(%s_id) do nothing;", tableName);
                }
                System.out.println(insertDataSql+dataSql);
                resInsertSql.add(insertDataSql + dataSql);
            } else {
                dataSql = dataSql + ",";
            }

        }

        return resInsertSql;
    }

    private String getCreateTableSql(String tableName, ArrayList<String> fieldArray, ArrayList<String> fieldTypeArray) {
        String createTableSql = String.format("create table if not exists %s (", tableName);
        for (int i = 0; i < fieldArray.size(); i++) {
            if (i == fieldArray.size() - 1) {
                createTableSql = createTableSql + String.format("%s %s);", fieldArray.get(i), fieldTypeArray.get(i));
            } else {
                createTableSql = createTableSql + String.format("%s %s,", fieldArray.get(i), fieldTypeArray.get(i));
            }

        }
        return createTableSql;
    }

    private ArrayList<String> initFieldList(String tableName) {
        String primaryKey;
        ArrayList<String> fieldList = new ArrayList<>();
        if (tableName.contains("branch")) {
             primaryKey = String.format("pk_%s_id", tableName);
        } else {
             primaryKey = String.format("%s_id", tableName);
        }
        fieldList.add(primaryKey);
        return fieldList;
    }

    private ArrayList<String> initFiledTypeList(String tableName) {
        String primaryKeyType;
        ArrayList<String> fieldTypeList = new ArrayList<>();
        if (tableName.contains("branch")) {
            primaryKeyType = String.format("int8 not null, CONSTRAINT %s_pkey PRIMARY KEY (pk_%s_id)", tableName, tableName);
        } else {
            primaryKeyType = String.format("int8 not null, CONSTRAINT %s_pkey PRIMARY KEY (%s_id)", tableName, tableName);
        }
        fieldTypeList.add(primaryKeyType);
        return fieldTypeList;
    }

    private String getTypeString(Map<String,String> customFieldTypeMap, JSONObject jsonObject, String k) {
        if (customFieldTypeMap !=null && customFieldTypeMap.containsKey(k)) {
            return customFieldTypeMap.get(k);
        }
        Object o = jsonObject.get(k);
        if (o instanceof JSONObject) {
            return "int8";
        } else if (o instanceof JSONArray) {
            if (((JSONArray) o).size()>0 && ((JSONArray) o).get(0) instanceof JSONObject) {
                return "int8[]";
            } else if (((JSONArray) o).size() > 0 && ((JSONArray) o).get(0) instanceof Number) {
                return "float4[]";
            } else {
                return "text[]";
            }
        } else if (o instanceof Number) {
            return "float4";
        } else if (o instanceof String) {
            return "varchar";
        } else {
            return "text";
        }
    }
}
