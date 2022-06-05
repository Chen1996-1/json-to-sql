package cn.utils.jsonToSql;

import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Resource;

import static org.junit.jupiter.api.Assertions.*;


public class JsonToSqlServiceTest {

    private JsonToSqlService jsonToSqlService = new JsonToSqlServiceImpl();
    @Test
    public void testJsonToSql() {
        JsonToSqlInfo jsonToSqlInfo = new JsonToSqlInfo();
        jsonToSqlInfo.setFilePath("F:\\Desktop\\tem.json");
        String[] sqlArray  = jsonToSqlService.JsonToSql(jsonToSqlInfo);
        System.out.println("ok");
    }
}