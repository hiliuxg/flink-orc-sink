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
import java.nio.charset.Charset;
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

            ColumnVector cv = batch.cols[i];
            Object colVal = row.getField(i);

            if (colVal != null){
                String type = this.orcSchema.getOrcFieldType(i) ;
                switch (type) {
                    case OrcSchema.OrcType.TYPE_BOOLEAN :
                    case OrcSchema.OrcType.TYPE_BIGINT :
                    case OrcSchema.OrcType.TYPE_INT :
                    case OrcSchema.OrcType.TYPE_TINYINT :
                        LongColumnVector longCv = (LongColumnVector) cv;
                        longCv.vector[index] = Long.parseLong(colVal.toString());
                        break ;
                    case OrcSchema.OrcType.TYPE_DOUBLE :
                    case OrcSchema.OrcType.TYPE_FLOAT :
                        DoubleColumnVector doubleCv = (DoubleColumnVector) cv;
                        doubleCv.vector[index] = Double.parseDouble(colVal.toString());
                        break ;
                    case OrcSchema.OrcType.TYPE_STRING :
                        BytesColumnVector bytesCv = (BytesColumnVector) cv;
                        bytesCv.setVal(index,colVal.toString().getBytes(Charset.forName("utf-8")));
                        break ;
                    case OrcSchema.OrcType.TYPE_TIMESTAMP :
                        TimestampColumnVector timestampCv = (TimestampColumnVector) cv;
                        Timestamp timestamp = (Timestamp)colVal ;
                        timestampCv.time[index] = timestamp.getTime();
                        timestampCv.nanos[index] = timestamp.getNanos() ;
                        break ;
                    case OrcSchema.OrcType.TYPE_DECIMAL :
                        DecimalColumnVector decimalCv= (DecimalColumnVector) cv;
                        BigDecimal bigDecimal = (BigDecimal)colVal;
                        HiveDecimal hiveDecimal = HiveDecimal.create(bigDecimal);
                        HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(hiveDecimal);
                        decimalCv.precision = Short.valueOf(bigDecimal.precision() + "");
                        decimalCv.scale = Short.valueOf(bigDecimal.scale() + "");
                        decimalCv.vector[index] = hiveDecimalWritable ;
                        break ;
                    default:
                        throw new RuntimeException("the " + type + " type not support to orc now") ;
                }
            }else{
                cv.noNulls = false ;
                cv.isNull[index] = true ;
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
