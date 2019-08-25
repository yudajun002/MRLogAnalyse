package com.xiaoyu.transformer.mr;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.xiaoyu.common.GlobalConstants;
import com.xiaoyu.common.KpiType;
import com.xiaoyu.transformer.model.dim.base.BaseDimension;
import com.xiaoyu.transformer.model.value.BaseStatsValueWritable;
import com.xiaoyu.transformer.service.IDimensionConverter;
import com.xiaoyu.transformer.service.impl.DimensionConverterImpl;
import com.xiaoyu.util.JdbcManager;

/**
 * 自定义输出到mysql的outputformat类
 * BaseDimension:reducer输出的key,顶级父类key
 * BaseStatsValueWritable：reducer输出的value，顶级父类value
 *
 */
public class TransformerOutputFormat extends OutputFormat<BaseDimension, BaseStatsValueWritable> {
    private static final Logger logger = Logger.getLogger(TransformerOutputFormat.class);

    /**
     * 定义每条数据的输出格式，一条数据就是reducer任务每次执行write方法输出的数据。
     */
    @Override
	public RecordWriter<BaseDimension, BaseStatsValueWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Connection conn = null;
        IDimensionConverter converter = new DimensionConverterImpl();
        try {
        	//jdbc连接配置获取
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            conn.setAutoCommit(false);//关闭自动提交，使用批量提交处理
        } catch (SQLException e) {
            logger.error("获取数据库连接失败", e);
            throw new IOException("获取数据库连接失败", e);
        }
        return new TransformerRecordWriter(conn, conf, converter);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // 检测输出空间，输出到mysql不用检测
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }

    /**
     * 自定义具体数据输出writer
     *
     */
    public class TransformerRecordWriter extends RecordWriter<BaseDimension, BaseStatsValueWritable> {
        private Connection conn = null;
        private Configuration conf = null;
        private IDimensionConverter converter = null;
        //统计维度，prepared sql
        private Map<KpiType, PreparedStatement> map = new HashMap<KpiType, PreparedStatement>();
        //每个维度计算数量（条数统计）批量统计
        private Map<KpiType, Integer> batch = new HashMap<KpiType, Integer>();

        public TransformerRecordWriter(Connection conn, Configuration conf, IDimensionConverter converter) {
            super();
            this.conn = conn;
            this.conf = conf;
            this.converter = converter;
        }

        
        /**
         * 当reduce任务输出数据是，由计算框架自动调用。把reducer输出的数据写到mysql中
         */
        @Override
        public void write(BaseDimension key, BaseStatsValueWritable value) throws IOException, InterruptedException {
            if (key == null || value == null) {
                return;
            }

            try {
                KpiType kpi = value.getKpi();
                PreparedStatement pstmt = null;//每一个pstmt对象对应一个sql语句
                int count = 1;//统计个数数值
                if (map.get(kpi) == null) {
                    // 使用kpi进行区分，从配置文件获取对应执行的sql命令，详见query-mapping.xml
                    pstmt = this.conn.prepareStatement(conf.get(kpi.name));
                    map.put(kpi, pstmt);
                } else {//批量统计处理
                    pstmt = map.get(kpi);
                    count = batch.get(kpi);
                    count++;
                }
                batch.put(kpi, count); // 批量次数的存储

                //设置上面获取到的sql命令所匹配的value,详见,output-collector.xml
                String collectorName = conf.get(GlobalConstants.OUTPUT_COLLECTOR_KEY_PREFIX + kpi.name);
                Class<?> clazz = Class.forName(collectorName);
                //转换不同子类实现类处理value设置，例：com.xiaoyu.transformer.mr.nu
                IOutputCollector collector = (IOutputCollector) clazz.newInstance();
                //把value插入到mysql的方法。由于kpi维度不一样。插入到不同的表里面
                collector.collect(conf, key, value, pstmt, converter);

                if (count % Integer.valueOf(conf.get(GlobalConstants.JDBC_BATCH_NUMBER, GlobalConstants.DEFAULT_JDBC_BATCH_NUMBER)) == 0) {
                    pstmt.executeBatch();//执行批量提交
                    conn.commit();
                    batch.put(kpi, 0); // 重新统计批量条数
                }
            } catch (Throwable e) {
                logger.error("在writer中写数据出现异常", e);
                throw new IOException(e);
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                for (Map.Entry<KpiType, PreparedStatement> entry : this.map.entrySet()) {
                    entry.getValue().executeBatch();
                }
            } catch (SQLException e) {
                logger.error("执行executeUpdate方法异常", e);
                throw new IOException(e);
            } finally {
                try {
                    if (conn != null) {
                        conn.commit(); // 进行connection的提交动作
                    }
                } catch (Exception e) {
                    // nothing
                } finally {
                    for (Map.Entry<KpiType, PreparedStatement> entry : this.map.entrySet()) {
                        try {
                            entry.getValue().close();
                        } catch (SQLException e) {
                            // nothing
                        }
                    }
                    if (conn != null)
                        try {
                            conn.close();
                        } catch (Exception e) {
                            // nothing
                        }
                }
            }
        }

    }
    
    
}
