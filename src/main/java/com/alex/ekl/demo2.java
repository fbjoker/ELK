package com.alex.ekl;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class demo2 {
    private TransportClient client;

    @Before
    @Test
    public void test1() throws UnknownHostException {
        //获取连接
        //1获取配置文件
        Settings set = Settings.builder().put("cluster.name","my-application").build();

        //2创建client
         client = new PreBuiltTransportClient(set);
        //3主机ip和端口号,API端口是9300,9200是web的端口
        client.addTransportAddress( new InetSocketTransportAddress(InetAddress.getByName("hadoop102"),9300));
        //
       // System.out.println(client.toString());

    }

    //创建索引
    @Test
    public void createindex(){
        // 1 创建索引
        client.admin().indices().prepareCreate("blog2").get();
        // 2 关闭连接
        client.close();


    }


    //添加数据
    @Test
    public void createIndexByJson() throws UnknownHostException {

        // 1 文档数据准备
        String json = "{" + "\"id\":\"1\"," + "\"title\":\"基于Lucene的搜索服务器\","
                + "\"content\":\"基于RESTful web接口\"" + "}";

        // 2 创建文档 指定index type docid
        IndexResponse indexResponse = client.prepareIndex("blog2", "article", "1").setSource(json).execute().actionGet();


        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }


    //查询
    @Test
    public void getData() throws Exception {

        // 1 查询文档
        GetResponse response = client.prepareGet("blog2", "article", "1").get();

        // 2 打印搜索的结果
        System.out.println(response.getSourceAsString());

        // 3 关闭连接
        client.close();
    }



}
