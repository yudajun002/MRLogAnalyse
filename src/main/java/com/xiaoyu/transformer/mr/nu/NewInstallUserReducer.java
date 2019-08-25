package com.xiaoyu.transformer.mr.nu;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.xiaoyu.common.KpiType;
import com.xiaoyu.transformer.model.dim.StatsUserDimension;
import com.xiaoyu.transformer.model.value.map.TimeOutputValue;
import com.xiaoyu.transformer.model.value.reduce.MapWritableValue;

/**
 * 计算new isntall user的reduce类
 * 
 *
 */
public class NewInstallUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    private MapWritableValue outputValue = new MapWritableValue();
    private Set<String> unique = new HashSet<String>();//set去除重复处理

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        this.unique.clear();

        // 开始计算uuid的个数,set去除重复处理
        for (TimeOutputValue value : values) {
            this.unique.add(value.getId());//uid,用户ID
        }
        
        MapWritable map = new MapWritable();//相当于java中HashMap
        //分类结果，根据map key获取结果值
        map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
        outputValue.setValue(map);

        // 设置kpi名称,插入哪个数据表
        String kpiName = key.getStatsCommon().getKpi().getKpiName();
        if (KpiType.NEW_INSTALL_USER.name.equals(kpiName)) {
            // 计算stats_user表中的新增用户,插入表user_install table
            outputValue.setKpi(KpiType.NEW_INSTALL_USER);
        } else if (KpiType.BROWSER_NEW_INSTALL_USER.name.equals(kpiName)) {
            // 计算stats_device_browser表中的新增用户,插入browser——install table
            outputValue.setKpi(KpiType.BROWSER_NEW_INSTALL_USER);
        }
        context.write(key, outputValue);
    }
}
