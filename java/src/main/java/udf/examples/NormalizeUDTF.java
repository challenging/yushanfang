package udf.examples;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.aliyun.odps.udf.OdpsType;
import com.aliyun.odps.udf.UDTF;

/***
 * 将一组实数规格化到0和1之间
 * 
 * 由于需要在全部数据上遍历多遍（先获取最小和最大值，再规格化），所以用内存数据结构缓存所有数据
 */
public class NormalizeUDTF extends UDTF {

	/**
	 * 临时保存所有结果的内存数据结构
	 */
	List<Double> values = new ArrayList<Double>();

	/**
	 * UDTF Initialize接口
	 * 
	 * 定义输入和输出数据类型
	 */
	public OdpsType[] initialize(OdpsType[] signature) throws Exception {
		if (signature.length == 1 || signature[0] == OdpsType.DOUBLE) {
			OdpsType ret[] = { OdpsType.DOUBLE };
			return ret;
		}

		return null;
	}

	/**
	 * UDTF Process接口
	 * 
	 * 每条记录都会调用此接口，这里仅仅简单将数据存入ArrayList
	 */
	public void process(Object[] args) throws Exception {
		values.add((Double) args[0]);
	}

	/**
	 * UDTF Close接口
	 * 
	 * 任务最后调用此接口，规格化所有数据并输出。forward方法用于输出结果
	 */
	public void close() throws Exception {
		Double min = Collections.min(values);
		Double max = Collections.max(values);

		for (Double e : values) {
			Object[] output = new Object[1];
			output[0] = min.equals(max) ? 0D : (e - m/Users/rungchi/Documents/Company/Migo/Projects/Alibaba/YSF Programs/MIGO_UDF_CDF/src/main/java/udf/examples/GcdUDF.javain) / (max - min);
			forward(output);
		}
	}

}
