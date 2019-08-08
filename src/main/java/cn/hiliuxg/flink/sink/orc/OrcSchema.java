package cn.hiliuxg.flink.sink.orc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OrcSchema implements Serializable {

     private final String[] fieldNames;
     private final TypeInformation[] flinkFieldTypes ;
     private Map<TypeInformation,String> orcFlinkTypeMapper ;

     public OrcSchema(String[] fieldNames,TypeInformation[] flinkFieldTypes){
         this.fieldNames = fieldNames ;
         this.flinkFieldTypes = flinkFieldTypes ;
         this.initOrcFlinkTypeMapper();
     }

     public int getFieldCount(){
         return  this.fieldNames.length ;
     }

     public String getFieldName(int index){
         return this.fieldNames[index];
     }

     public String getOrcFieldType(int index){
         return this.orcFlinkTypeMapper.get(this.flinkFieldTypes[index]);
     }


     @Override
     public String toString() {
        StringBuffer sb = new StringBuffer() ;
        sb.append("struct<") ;
        for (int i = 0 ; i < this.getFieldCount() ;i++){
            sb.append(this.getFieldName(i) + ":" + this.getOrcFieldType(i) + ",");
        }
        return  sb.substring(0,sb.length()-1) + ">";
     }

     private void initOrcFlinkTypeMapper(){
        orcFlinkTypeMapper = new HashMap<>() ;
        orcFlinkTypeMapper.put(Types.BYTE, OrcType.TYPE_STRING) ;
        orcFlinkTypeMapper.put(Types.STRING, OrcType.TYPE_STRING) ;
        orcFlinkTypeMapper.put(Types.BOOLEAN, OrcType.TYPE_BOOLEAN) ;
        orcFlinkTypeMapper.put(Types.SHORT, OrcType.TYPE_TINYINT) ;
        orcFlinkTypeMapper.put(Types.INT, OrcType.TYPE_INT) ;
        orcFlinkTypeMapper.put(Types.LONG, OrcType.TYPE_BIGINT) ;
        orcFlinkTypeMapper.put(Types.FLOAT, OrcType.TYPE_FLOAT) ;
        orcFlinkTypeMapper.put(Types.DOUBLE, OrcType.TYPE_DOUBLE) ;
        orcFlinkTypeMapper.put(Types.BIG_DEC, OrcType.TYPE_DECIMAL) ;
        orcFlinkTypeMapper.put(Types.SQL_TIMESTAMP, OrcType.TYPE_TIMESTAMP) ;
     }


    /**
     * orc type constants
     */
    static final class OrcType{
        public final static String TYPE_STRING = "string";
        public final static String TYPE_BOOLEAN = "boolean";
        public final static String TYPE_TINYINT = "tinyint";
        public final static String TYPE_INT = "int";
        public final static String TYPE_BIGINT = "bigint";
        public final static String TYPE_FLOAT = "float";
        public final static String TYPE_DOUBLE = "double";
        public final static String TYPE_DECIMAL = "decimal";
        public final static String TYPE_TIMESTAMP = "timestamp";
    }

}
