import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object Redis {
  private val config = new JedisPoolConfig()
  config.setMaxTotal(10)
  config.setMaxIdle(10)
  config.setTestOnBorrow(true)

  private val pool = new JedisPool(config, "172.16.9.154", 6379)

  def getConnection: Jedis = {
    pool.getResource
  }

  def close(): Unit = {
    pool.close()
  }
}
