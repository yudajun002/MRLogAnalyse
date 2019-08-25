package com.xiaoyu.transformer.mr.nu;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import com.xiaoyu.common.GlobalConstants;
import com.xiaoyu.transformer.model.dim.StatsUserDimension;
import com.xiaoyu.transformer.model.dim.base.BaseDimension;
import com.xiaoyu.transformer.model.value.BaseStatsValueWritable;
import com.xiaoyu.transformer.model.value.reduce.MapWritableValue;
import com.xiaoyu.transformer.mr.IOutputCollector;
import com.xiaoyu.transformer.service.IDimensionConverter;

public class StatsUserNewInstallUserCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, 
    		PreparedStatement pstmt, IDimensionConverter converter) 
    				throws SQLException, IOException {
    	
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        //提取reduce阶段统计的结果值，key = -1
        IntWritable newInstallUsers = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));

        int i = 0;
        //通过convert查询db内是否存在id，存在更新原数据，不存在则insert
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.addBatch();//往批处理放入数据
    }

}
