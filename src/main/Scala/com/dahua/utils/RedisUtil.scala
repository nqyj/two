package com.dahua.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisUtil {

  // 连接redis的工具方法。
  private val jedisPool = new JedisPool(new GenericObjectPoolConfig,"192.168.137.66",6379,30000,null,1)
  def getJedis = jedisPool.getResource

}
