package cn.hiliuxg.flink.sink.orc;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;


public class RowOrcBucketingSink extends RichSinkFunction<Row>
        implements CheckpointedFunction, CheckpointListener{

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RowOrcBucketingSink.class);

    /**
     * The base {@code Path} that stores all bucket directories.
     */
    private final String basePath;

    /**
     * The {@code Bucketer} that is used to determine the path of bucket directories.
     */
    private Bucketer<Row> bucketer;

    /**
     * We have a template and call duplicate() for each parallel writer in open() to get the actual
     * writer that is used for the part files.
     */
    private Writer writerTemplate;

    /**
     * The flink configuration
     */
    private Configuration fsConfig;

    /**
     * We use the state to keep track of bucket's life cycle
     */
    private transient RowOrcBucketingSink.State<Row> state ;

    /**
     * The hadoop file system client object
     */
    private transient FileSystem fs;


    private transient Long watermarkPerOrcBucketing;

    /**
     * Creates a new {@code RowOrcBucketingSink} that writes files to the given base directory.
     *
     * <p>This uses a{@link BasePathBucketer} as {@link Bucketer} and a {@link RowOrcWriter} has writer.
     *
     * @param basePath The directory to which to write the bucket files.
     * @param orcSchema The schema for the orc file
     */
    public RowOrcBucketingSink(String basePath, OrcSchema orcSchema) {
        this.basePath = basePath;
        this.bucketer = new BasePathBucketer<>();
        this.writerTemplate = new RowOrcWriter(orcSchema);
        this.fsConfig = new Configuration() ;
    }

    @Override
    public void open(Configuration parameters) {
        this.state = new RowOrcBucketingSink.State<>();
        RuntimeContext context = super.getRuntimeContext();
        context.getMetricGroup().gauge("watermarkPerOrcBucketing", (Gauge<Long>) () -> watermarkPerOrcBucketing);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws IOException {
        if (fs == null) {
            Path path = new Path(basePath);
            fs = BucketingSink.createHadoopFileSystem(path, fsConfig);
        }
    }

    @Override
    public void close() {
        synchronized (this.state) {
            try{
                if (fs != null){
                    fs.close();
                }
            }catch (Exception ex){}
        }
    }

    @Override
    public void invoke(Row value,Context context) throws Exception {
        synchronized (this.state){
            Path bucketPath = bucketer.getBucketPath(null, new Path(basePath), value);
            RowOrcBucketingSink.BucketState<Row> bucketState = this.findBucketState(bucketPath) ;
            bucketState.writer.write(value);
            bucketState.currentWatermark = context.currentWatermark();
        }
    }

    /**
     * find or create a BucketState
     * @param bucketPath
     * @return
     * @throws IOException
     */
    private RowOrcBucketingSink.BucketState<Row> findBucketState(Path bucketPath) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        String pathKey = bucketPath.toString();
        RowOrcBucketingSink.BucketState<Row> bucketState = this.state.getBucketState(pathKey);
        if (bucketState == null) {
            bucketState = new RowOrcBucketingSink.BucketState<>();
            Path currentFile ;
            Path pendingFile ;
            Path partFile ;
            while (!bucketState.isWriterOpen){
                currentFile = this.assembleProcessFile(bucketPath, subtaskIndex, this.state.cur(pathKey));
                pendingFile = this.assemblePendingFile(currentFile) ;
                partFile = this.assemblePartFile(pendingFile) ;
                if (fs.exists(currentFile) || fs.exists(pendingFile) || fs.exists(partFile)){
                    LOG.warn("file  exists ,current counter is {} , step by 1" , this.state.cur(pathKey));
                }else{
                    bucketState.writer = writerTemplate.duplicate();
                    bucketState.writer.open(fs,currentFile);
                    bucketState.isWriterOpen = true;
                    bucketState.currentFile = currentFile;
                }
                //when the file exists , counter increase by  1
                this.state.inc(pathKey);
            }
            this.state.addBucketState(pathKey, bucketState);
        }
        return bucketState ;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        synchronized (this.state) {
            try {
                Iterator<Map.Entry<String, RowOrcBucketingSink.BucketState<Row>>> bucketStatesIt
                        = this.state.bucketStates.entrySet().iterator();
                while (bucketStatesIt.hasNext()) {
                    RowOrcBucketingSink.BucketState<Row> bucketState = bucketStatesIt.next().getValue();
                    closeAndMoveProcessFile(bucketState,context.getCheckpointId());
                    this.state.putWatermark(context.getCheckpointId(),bucketState.currentWatermark) ;
                    bucketStatesIt.remove();
                }
            }catch (Exception ex){
                this.state.removePendingFile(context.getCheckpointId());
                throw ex ;
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        synchronized (this.state) {
            this.movePendingFile(checkpointId);
        }
    }

    /**
     * Move the current checkpointId pending file to final part file
     * @param checkpointId
     * @throws IOException
     */
    private void movePendingFile(long checkpointId) throws IOException {
        List<Path> pendingFiles = this.state.pendingFilesPerCheckpoint.get(checkpointId);
        if (pendingFiles != null){
            for (Path pendingFile : pendingFiles){
                Path partFile = this.assemblePartFile(pendingFile) ;
                if (!fs.exists(partFile)){
                    fs.rename(pendingFile,partFile) ;
                }
                LOG.info("move pending file , {} to {}" ,checkpointId,pendingFile,partFile);
            }
            this.state.removePendingFile(checkpointId) ;

            //convenion to see in promethues monitor ,due to the UTC Time . In china ,we minus 8 hours
            this.watermarkPerOrcBucketing = this.state.getWatermark(checkpointId) - Time.hours(8).toMilliseconds();
            this.state.removeWatermark(checkpointId);
        }
    }

    /**
     * Close the current part file and moves it from the in-progress state to the pending state.
     */
    private void closeAndMoveProcessFile(RowOrcBucketingSink.BucketState<Row> bucketState,Long checkpointId) throws Exception {
        if (bucketState.isWriterOpen){
            bucketState.writer.close();
            Path pendingFile = this.assemblePendingFile(bucketState.currentFile);
            fs.rename(bucketState.currentFile,pendingFile);
            this.state.addPendingFile(checkpointId,pendingFile);
            LOG.info("has close current file  {}, move it to {} " , bucketState.currentFile,pendingFile);
        }
    }

    private Path assemblePartFile(Path pendingFile) {
        String fileName = pendingFile.getName().replaceFirst("_","")
                .replaceAll(".in-pending","");
        return new Path(pendingFile.getParent(), fileName);
    }

    private Path assemblePendingFile(Path processFile){
        String fileName = processFile.getName().replaceAll("in-process","in-pending");
        return new Path(processFile.getParent(), fileName);
    }

    private Path assembleProcessFile(Path bucket, int subtaskIndex, int partIndex) {
        return new Path(bucket, String.format("%s-%s-%s-%s", "_flink", subtaskIndex, partIndex, "part.in-process"));
    }

    // --------------------------------------------------------------------------------------------
    //  Setters for User configuration values
    // --------------------------------------------------------------------------------------------

    /**
     * Specify a custom {@code Configuration} that will be used when creating
     * the {@link FileSystem} for writing.
     */
    public RowOrcBucketingSink setFSConfig(org.apache.hadoop.conf.Configuration config) {
        this.fsConfig = new Configuration();
        for (Map.Entry<String, String> entry : config) {
            fsConfig.setString(entry.getKey(), entry.getValue());
        }
        return this;
    }

    /**
     * Sets the {@link Bucketer} to use for determining the bucket files to write to.
     *
     * @param bucketer The bucketer to use.
     */
    public RowOrcBucketingSink setBucketer(Bucketer<Row> bucketer) {
        this.bucketer = bucketer;
        return this;
    }

    /**
     * The flink configuration to hdfs configuration
     * @param flinkConf
     * @return
     */
    private org.apache.hadoop.conf.Configuration confTrans(Configuration flinkConf){
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        if (flinkConf != null) {
            for (String key : flinkConf.keySet()) {
                hadoopConf.set(key, flinkConf.getString(key, null));
            }
        }
        return hadoopConf ;
    }

    /**
     * We use the state to keep track of bucket's life cycle
     * @param <Row>
     */
    static final class State<Row> implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * We use it to store the bucket , one diretory one bucket .
         * And we will remove it when the checkpoint snapshot finish
         */
        private final Map<String, RowOrcBucketingSink.BucketState<Row>> bucketStates = new HashMap<>();

        /**
         * Use for storing the pending file when checkpoint snapshot finish
         */
        private final Map<Long, List<Path>> pendingFilesPerCheckpoint = new HashMap<>();

        /**
         * When checkpoint finish ,we send the current watermark metric to exporter
         */
        private final Map<Long,Long> watermarkPerCheckpoint = new HashMap<>() ;

        /**
         * We use it to store the newly open bucket index
         * The map key is directory and the value is index
         * See the method inc and cur
         */
        private final Map<String,Integer> bucketPartCounter = new HashMap<>() ;


        void inc(String bucketPath){
            Integer counter  = bucketPartCounter.get(bucketPath) ;
            if (counter == null){
                counter = 0;
            }
            bucketPartCounter.put(bucketPath,++counter) ;
        }

        int cur(String bucketPath){
            Integer counter = bucketPartCounter.get(bucketPath)  ;
            if (counter == null){
                counter = 0 ;
            }
            return counter ;
        }

        Long getWatermark(Long checkpointId ){
            return this.watermarkPerCheckpoint.get(checkpointId);
        }

        void putWatermark(Long checkpointId,Long watermark ){
            this.watermarkPerCheckpoint.put(checkpointId,watermark);
        }

        void removeWatermark(Long checkpointId){
            this.watermarkPerCheckpoint.remove(checkpointId) ;
        }

        void removePendingFile(Long checkpointId){
            this.pendingFilesPerCheckpoint.remove(checkpointId) ;
        }

        void addPendingFile(Long checkpointId,Path pendingFile){
            List<Path> pendingFileList = this.pendingFilesPerCheckpoint.get(checkpointId);
            if (pendingFileList == null){
                pendingFileList = new ArrayList<>();
            }
            pendingFileList.add(pendingFile) ;
            this.pendingFilesPerCheckpoint.put(checkpointId,pendingFileList) ;
        }

        void addBucketState(String bucketPath, RowOrcBucketingSink.BucketState<Row> bucketState) {
            bucketStates.put(bucketPath, bucketState);
        }

        RowOrcBucketingSink.BucketState<Row> getBucketState(String bucketPath) {
            return bucketStates.get(bucketPath);
        }

        @Override
        public String toString() {
            return bucketStates.toString();
        }
    }

    /**
     * This is used for keeping track of the current in-progress buckets and files that we mark
     * for moving from pending to final location after we get a checkpoint-complete notification.
     */
    static final class BucketState<Row> implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * Tracks if the writer is currently opened or closed.
         */
        private boolean isWriterOpen = false;

        private long currentWatermark = Long.MIN_VALUE ;

        /**
         * The in-process file  writting currently
         */
        private Path currentFile ;

        /**
         * The actual writer that we user for writing the part files.
         */
        private Writer<Row> writer;

    }

}
