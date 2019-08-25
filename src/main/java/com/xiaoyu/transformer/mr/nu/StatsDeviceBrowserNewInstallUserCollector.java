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

public class StatsDeviceBrowserNewInstallUserCollector implements IOutputCollector {

    @Override
    /**
     * 给sql语句中的？赋值的方法
     */
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, 
    		PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        IntWritable newInstallUsers = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getBrowser()));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.addBatch();
    }

}
