package com.xiaoyu.transformer.mr.au;

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

/**
 * browser active collect
 * sql 数值填充，
 */
public class ActiveUserBrowserCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
     // 进行强制后获取对应的值
        StatsUserDimension statsUser = (StatsUserDimension) key;
        IntWritable activeUserValue = (IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-1));

        // 进行参数设置
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getBrowser()));
        pstmt.setInt(++i, activeUserValue.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, activeUserValue.get());

        // 添加到batch中
        pstmt.addBatch();
    }

}
