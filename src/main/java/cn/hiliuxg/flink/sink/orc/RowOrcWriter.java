package cn.hiliuxg.flink.sink.orc;

import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;

public class RowOrcWriter implements Writer<Row> {

    private org.apache.orc.Writer orcWriter ;
    private VectorizedRowBatch batch;
    private OrcSchema orcSchema ;

    public RowOrcWriter(OrcSchema orcSchema) {
        this.orcSchema = orcSchema ;
    }

    protected RowOrcWriter(RowOrcWriter other) {
        this.orcSchema = other.orcSchema;
    }

    @Override
    public void open(FileSystem fs , Path path) throws IOException {
        Configuration conf = fs.getConf();
        TypeDescription schema = TypeDescription.fromString(this.orcSchema.toString());
        OrcFile.WriterOptions opts = OrcFile.writerOptions(conf).setSchema(schema).fileSystem(fs);
        this.orcWriter = OrcFile.createWriter(path, opts);
        this.batch = schema.createRowBatch();
    }

    @Override
    public long flush() throws IOException {
        return 0;
    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }


    @Override
    public void close() throws IOException {
        if (this.batch.size != 0){
            this.orcWriter.addRowBatch(batch);
            this.batch.reset();
        }
        this.orcWriter.close();
    }


    @Override
    public void write(Row row) throws IOException {

        int index = this.batch.size++;

        //fill up the orc value
        for (int i = 0 ; i < this.orcSchema.getFieldCount() ;i++){
            String type = this.orcSchema.getOrcFieldType(i) ;
            switch (type) {
                case OrcSchema.OrcType.TYPE_BOOLEAN :
                    LongColumnVector booleanColumnVector = (LongColumnVector) batch.cols[i];
                    booleanColumnVector.vector[index] = Long.parseLong(((Boolean)row.getField(i)) ? "1" : "0" );
                    break ;
                case OrcSchema.OrcType.TYPE_BIGINT :
                    LongColumnVector bigintColumnVector = (LongColumnVector) batch.cols[i];
                    bigintColumnVector.vector[index] = (Long)row.getField(i);
                    break ;
                case OrcSchema.OrcType.TYPE_DECIMAL :
                    DecimalColumnVector decimalColumnVector = (DecimalColumnVector) batch.cols[i];
                    BigDecimal bigDecimal = (BigDecimal)row.getField(i);
                    HiveDecimal hiveDecimal = HiveDecimal.create(bigDecimal);
                    HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(hiveDecimal);
                    decimalColumnVector.precision = Short.valueOf(bigDecimal.precision() + "");
                    decimalColumnVector.scale = Short.valueOf(bigDecimal.scale() + "");
                    decimalColumnVector.vector[index] = hiveDecimalWritable ;
                    break ;
                case OrcSchema.OrcType.TYPE_DOUBLE :
                    DoubleColumnVector doubleColumnVector = (DoubleColumnVector) batch.cols[i];
                    doubleColumnVector.vector[index] = (Double)row.getField(i);
                    break ;
                case OrcSchema.OrcType.TYPE_FLOAT :
                    DoubleColumnVector floatColumnVector = (DoubleColumnVector) batch.cols[i];
                    floatColumnVector.vector[index] = Double.parseDouble(row.getField(i).toString());
                    break ;
                case OrcSchema.OrcType.TYPE_INT :
                    LongColumnVector intColumnVector = (LongColumnVector) batch.cols[i];
                    intColumnVector.vector[index] = Long.parseLong(row.getField(i).toString());
                    break ;
                case OrcSchema.OrcType.TYPE_TINYINT :
                    LongColumnVector tinyColumnVector = (LongColumnVector) batch.cols[i];
                    tinyColumnVector.vector[index] = Long.parseLong(row.getField(i).toString());
                    break ;
                case OrcSchema.OrcType.TYPE_STRING :
                    BytesColumnVector stringColumnVector = (BytesColumnVector) batch.cols[i];
                    stringColumnVector.setVal(index,row.getField(i).toString().getBytes());
                    break ;
                case OrcSchema.OrcType.TYPE_TIMESTAMP :
                    TimestampColumnVector timestampColumnVector = (TimestampColumnVector) batch.cols[i];
                    Timestamp timestamp = (Timestamp)row.getField(i) ;
                    timestampColumnVector.time[index] = timestamp.getTime();
                    timestampColumnVector.nanos[index] = timestamp.getNanos() ;
                    break ;
                    default:
                        throw new RuntimeException("the flink type " + type + " not support to orc now") ;
            }
        }

        if (this.batch.size == VectorizedRowBatch.DEFAULT_SIZE){
            this.orcWriter.addRowBatch(batch);
            this.batch.reset();
        }

    }

    @Override
    public Writer<Row> duplicate() {
        return new RowOrcWriter(this);
    }

}
