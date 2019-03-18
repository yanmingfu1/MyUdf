package com.lee.MapMerge;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.HashMap;
import java.util.Map;

public class MapMerge extends UDF {
    public Map<String, String> evaluate(Map<String, String> map1, Map<String, String> map2) {
        for (String key : map1.keySet()) {
            //获取map1 key对应的value
            String ma1Value = map1.get(key);
            //判断key与value的值
            if (key == null && ma1Value == null) {
                continue;
            } else if (key != null && ma1Value == null) {
                ma1Value = "null";
            }
            //map 的合并
            String ma2Value = map2.get(key);  //获取map2 key对应的value
            if(ma2Value==null){  //判断map2 key对应的value是否为null
                ma2Value="null";
            }
            if (map2.containsKey(key)) {
                map2.put(key, ma2Value + "-" + ma1Value);
            } else {
                map2.put(key, ma1Value);
            }
        }
        //删除key和value都是null的情况
        if(map2.containsKey(null)&&map2.get(null)==null){
            map2.remove(null);
        }
        return map2;
    }

   /** public static void main(String[] args) {
     Map<String, String> map1=new HashMap<>();
     map1.put(null,null);
     map1.put("1",null);
     map1.put("2","2");
     Map<String, String> map2=new HashMap<>();
     map2.put(null,null);
     map2.put("1",null);
     map2.put("2","2");
     Map<String, String> mergeMap =new MapMerge().evaluate(map1,map2);
     for(String key : mergeMap.keySet()){
     System.out.println(key+","+mergeMap.get(key));
     }
     }*/
}
