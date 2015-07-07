package udf;

import com.aliyun.odps.udf.OdpsType;
import com.aliyun.odps.udf.UDTF;

/***
 * 御膳房UDTF
 */
public class MyUDTF extends UDTF {

    /**
     * UDTF Initialize接口
     * 
     * 定义输入和输出数据类型
     */
    public OdpsType[] initialize(OdpsType[] signature) throws Exception {
        // TODO: 检查输入参数

        return null;
    }

    /**
     * UDTF Process接口
     * 
     * 每条记录都会调用此接口。
     */
    public void process(Object[] args) throws Exception {
        // TODO: 实现对每条记录的处理逻辑
    }

    /**
     * UDTF Close接口
     * 
     * 任务最后调用此接口，规格化所有数据并输出。forward方法用于输出结果
     */
    public void close() throws Exception {
        // TODO: 实现终结逻辑
    }

}
