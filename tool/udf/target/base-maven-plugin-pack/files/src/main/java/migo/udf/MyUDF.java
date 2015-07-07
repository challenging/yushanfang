package migo.udf;

import com.aliyun.odps.udf.UDF;

/***
 * BASE UDF
 */
public final class MyUDF extends UDF {
    /**
     * UDF Evaluate接口
     * 
     * UDF在记录层面上是一对一，字段上是一对一或多对一。 Evaluate方法在每条记录上被调用一次，输入为一个或多个字段，输出为一个字段
     */
	public Double evaluate(Double x) {
    	double z = 0.0;
    	
    	if (Math.abs(x) >= 15) {
    		if (x > 0) {
    			return 1.0;
    		} else {
    			return 0.0;
    		}
    	} else {
    		z = Math.abs(x);
    	}
    	
        double p = 0.2316419;
        double b1 = 0.319381530;
        double b2 = -0.356563782;
        double b3 = 1.781477937; 
        double b4 = -1.821255978; 
        double b5 = 1.330274429; 
        	
        double t = 1.0 / (1.0 + p*z);
        
        double cd = 1.0 - pdf(z) * (b1*t + b2*Math.pow(t, 2) + b3*Math.pow(t, 3) + b4*Math.pow(t, 4) + b5*Math.pow(t, 5));

        if (x>0) {
        	return cd;
        } else {
        	return 1.0-cd;
        }
    }
    
    public double pdf(double x){
    	return Math.exp(-0.5*x*x/Math.sqrt(2*Math.PI));
    }
}
