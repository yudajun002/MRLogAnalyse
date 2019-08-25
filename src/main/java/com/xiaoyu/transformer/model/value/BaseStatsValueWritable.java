package com.xiaoyu.transformer.model.value;

import com.xiaoyu.common.KpiType;
import org.apache.hadoop.io.Writable;

import com.xiaoyu.common.KpiType;

/**
 * 自定义顶级的输出value父类
 *
 */
public abstract class BaseStatsValueWritable implements Writable {
    /**
     * 获取当前value对应的kpi值
     * 
     * @return
     */
    public abstract KpiType getKpi();
}
