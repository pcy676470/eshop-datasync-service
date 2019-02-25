package com.roncoo.eshop.datasync.rabbitmq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.roncoo.eshop.datasync.service.EshopProductService;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


/**
 * 数据同步服务，就是获取各种原子数据的变更消息
 * 
 * （1）然后通过spring cloud fegion调用eshop-product-service服务的各种接口， 获取数据
 * （2）将原子数据在redis中进行增删改
 * （3）将维度数据变化消息写入rabbitmq中另外一个queue，供数据聚合服务来消费
 * 
 * @author Administrator
 *
 */
@Component
@RabbitListener(queues = "data-change-queue")
public class DataChangeQueueReceiver {

	@Autowired
	private EshopProductService eshopProductService;
	
	@Autowired
	private JedisPool jedisPool;
	
	@Autowired
	private RabbitMQSender rabbitMQSender;
	
	private Set<String> dimDataChangeMessageSet = Collections.synchronizedSet(new HashSet<String>());
	
	private List<JSONObject> brandDataChangeMessageList = new ArrayList<JSONObject>();
	
	public DataChangeQueueReceiver(){
		new SendThread().start();
	}
	
	@RabbitHandler
	public void proccess(String message){
		
		JSONObject jsonObject = JSONObject.parseObject(message);
		String dataType = jsonObject.getString("data_type");
		
		if("brand".equals(dataType)){//品牌管理
			processBrandDataChangeMessage(jsonObject);
		} else if("category".equals(dataType)) {//分类管理
    		processCategoryDataChangeMessage(jsonObject); 
    	} else if("product_intro".equals(dataType)) {//商品介绍管理
    		processProductIntroDataChangeMessage(jsonObject); 
    	} else if("product_property".equals(dataType)) {//商品属性管理
    		processProductPropertyDataChangeMessage(jsonObject);
     	} else if("product".equals(dataType)) {//商品基本信息管理
     		processProductDataChangeMessage(jsonObject); 
     	} else if("product_specification".equals(dataType)) {//商品规格管理
     		processProductSpecificationDataChangeMessage(jsonObject);  
     	}
		
	}
	
	public void processBrandDataChangeMessage(JSONObject messageJSONObject){//品牌管理
		
		Long id = messageJSONObject.getLong("id");
		String eventType = messageJSONObject.getString("event_type");
		
		if("add".equals(eventType) || "update".equals(eventType)){
			
			brandDataChangeMessageList.add(messageJSONObject);
			System.out.println("【将品牌数据放入内存list中】,list.size=" + brandDataChangeMessageList.size());
			
			if(brandDataChangeMessageList.size()>=2){
				System.out.println("【将品牌数据内存list大小大于等于2，开始执行批量调用】");
				String ids = "";
				
				for(int i=0;i<brandDataChangeMessageList.size();i++){
					ids += brandDataChangeMessageList.get(i).getLong("id");
					if(i<brandDataChangeMessageList.size()-1){
						ids += ",";
					}
				}
				
				System.out.println("【品牌数据ids生成】ids=" + ids); 
				JSONArray brandJSONArray = JSONArray.parseArray(eshopProductService.findBrandByIds(ids));
				System.out.println("【通过批量调用获取到品牌数据】jsonArray=" + brandJSONArray.toJSONString()); 
				
				for(int i=0;i<brandJSONArray.size();i++){
					JSONObject dataJSONObject = brandJSONArray.getJSONObject(i);
					Jedis jedis = jedisPool.getResource();
					jedis.set("brand_"+dataJSONObject.getLong("id"), dataJSONObject.toJSONString());
					
					System.out.println("【将品牌数据写入redis】brandId=" + dataJSONObject.getLong("id"));
					dimDataChangeMessageSet.add("{\"dim_type\": \"brand\", \"id\": " + dataJSONObject.getLong("id") + "}");
					System.out.println("【将品牌数据写入内存去重set中】brandId=" + dataJSONObject.getLong("id"));
				}
				brandDataChangeMessageList.clear();
				
			}
		}else if("delete".equals(eventType)){
			Jedis jedis = jedisPool.getResource();
			jedis.del("brand_"+id);
			dimDataChangeMessageSet.add("{\"dim_type\": \"brand\", \"id\": " + id + "}");
		}
		
		//rabbitMQSender.send("aggr-data-change-queue", "{\"dim_type\": \"brand\", \"id\": " + id + "}");
		//dimDataChangeMessageSet.add("{\"dim_type\": \"brand\", \"id\": " + id + "}");
	}
	
	private void processCategoryDataChangeMessage(JSONObject messageJSONObject) {//分类管理
		
		Long id = messageJSONObject.getLong("id");
		String eventType = messageJSONObject.getString("event_type");
		
		if("add".equals(eventType) || "update".equals(eventType)){
			JSONObject dataJSONObject = JSONObject.parseObject(eshopProductService.findCategoryById(id));
			Jedis jedis = jedisPool.getResource();
			jedis.set("category_"+dataJSONObject.getLong("id"), dataJSONObject.toJSONString());
		}else if("delete".equals(eventType)){
			Jedis jedis = jedisPool.getResource();
			jedis.del("category_"+id);
		}
		
		//rabbitMQSender.send("aggr-data-change-queue", "{\"dim_type\": \"category\", \"id\": " + id + "}");
		dimDataChangeMessageSet.add("{\"dim_type\": \"category\", \"id\": " + id + "}");
	}
	
	private void processProductIntroDataChangeMessage(JSONObject messageJSONObject) {//商品介绍管理

		Long id = messageJSONObject.getLong("id");
		Long productId = messageJSONObject.getLong("product_id");
		String eventType = messageJSONObject.getString("event_type");
		
		
		if("add".equals(eventType) || "update".equals(eventType)){
			JSONObject dataJSONObject = JSONObject.parseObject(eshopProductService.findProductIntroById(id));
			System.out.println("obj:"+eshopProductService.findProductIntroById(id));
			Jedis jedis = jedisPool.getResource();
			jedis.set("product_intro_"+dataJSONObject.getLong("id"), dataJSONObject.toJSONString());
		}else if("delete".equals(eventType)){
			Jedis jedis = jedisPool.getResource();
			jedis.del("product_intro_"+id);
		}
		
		//rabbitMQSender.send("aggr-data-change-queue", "{\"dim_type\": \"product_intro\", \"id\": " + productId + "}");
		dimDataChangeMessageSet.add("{\"dim_type\": \"product_intro\", \"id\": " + productId + "}");
	}
	
	  private void processProductDataChangeMessage(JSONObject messageJSONObject) {//商品基本信息管理
	    	
		  Long id = messageJSONObject.getLong("id"); 
	      String eventType = messageJSONObject.getString("event_type"); 
	    	
	    	if("add".equals(eventType) || "update".equals(eventType)) { 
	    		JSONObject dataJSONObject = JSONObject.parseObject(eshopProductService.findProductById(id));  
	    		Jedis jedis = jedisPool.getResource();
	    		jedis.set("product_" + id, dataJSONObject.toJSONString());
	    	} else if ("delete".equals(eventType)) {
	    		Jedis jedis = jedisPool.getResource();
	    		jedis.del("product_" + id);
	    	}
	    	  
	    	//rabbitMQSender.send("aggr-data-change-queue", "{\"dim_type\": \"product\", \"id\": " + id + "}");
	    	dimDataChangeMessageSet.add("{\"dim_type\": \"product\", \"id\": " + id + "}");
	    }
	    
	    private void processProductPropertyDataChangeMessage(JSONObject messageJSONObject) {//商品属性管理
	    	
	    	Long id = messageJSONObject.getLong("id"); 
	    	Long productId = messageJSONObject.getLong("product_id");
	    	String eventType = messageJSONObject.getString("event_type"); 
	    	
	    	if("add".equals(eventType) || "update".equals(eventType)) { 
	    		JSONObject dataJSONObject = JSONObject.parseObject(eshopProductService.findProductPropertyById(id));  
	    		Jedis jedis = jedisPool.getResource();
	    		jedis.set("product_property_" + productId, dataJSONObject.toJSONString());
	    	} else if ("delete".equals(eventType)) {
	    		Jedis jedis = jedisPool.getResource();
	    		jedis.del("product_property_" + productId);
	    	}
	    	  
	    	//rabbitMQSender.send("aggr-data-change-queue", "{\"dim_type\": \"product\", \"id\": " + productId + "}");
	    	dimDataChangeMessageSet.add("{\"dim_type\": \"product\", \"id\": " + productId + "}");
	    }
	    
	    private void processProductSpecificationDataChangeMessage(JSONObject messageJSONObject) {//商品规格管理
	    	
	    	Long id = messageJSONObject.getLong("id"); 
	    	Long productId = messageJSONObject.getLong("product_id");
	    	String eventType = messageJSONObject.getString("event_type"); 
	    	
	    	if("add".equals(eventType) || "update".equals(eventType)) { 
	    		JSONObject dataJSONObject = JSONObject.parseObject(eshopProductService.findProductSpecificationById(id));  
	    		Jedis jedis = jedisPool.getResource();
	    		jedis.set("product_specification_" + productId, dataJSONObject.toJSONString());
	    	} else if ("delete".equals(eventType)) {
	    		Jedis jedis = jedisPool.getResource();
	    		jedis.del("product_specification_" + productId);
	    	}
	    	  
	    	//rabbitMQSender.send("aggr-data-change-queue", "{\"dim_type\": \"product\", \"id\": " + productId + "}");
	    	dimDataChangeMessageSet.add("{\"dim_type\": \"product\", \"id\": " + productId + "}");
	    }
	
	
	    private class SendThread extends Thread{
	    	
	    	@Override
	    	public void run(){
	    		while(true){
	    			if(!dimDataChangeMessageSet.isEmpty()){
	    				for(String message:dimDataChangeMessageSet){
	    					rabbitMQSender.send("aggr-data-change-queue", message);
	    				}
	    				dimDataChangeMessageSet.clear();
	    			}
	    			try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
	    		}
	    	}
	    }
}
