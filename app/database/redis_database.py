import logging
import json
import time
import uuid

import valkey as redis
from app.utils import singleton
from . import settings

class Payload:
    def __init__(self, consumer, queue_name, group_name, msg_id, message):
        self.__consumer = consumer
        self.__queue_name = queue_name
        self.__group_name = group_name
        self.__msg_id = msg_id
        self.__message = json.loads(message["message"])

    def ack(self):
        try:
            self.__consumer.xack(self.__queue_name, self.__group_name, self.__msg_id)
            return True
        except Exception:
            logging.warning("[EXCEPTION]ack" + str(self.__queue_name) + "||" + str(e))
        return False
    
    def get_message(self):
        return self.__message
    
@singleton
class RedisDB:
    def __init__(self):
        self.REDIS = None
        self.config = settings.REDIS
        self.__open__()

    def __open__(self):
        try:
            self.REDIS = redis.StrictRedis(
                host=self.config["host"],
                port=int(self.config.get("port", "6379")),
                db=int(self.config.get("db", 1)),
                password=self.config.get("password"),
                decode_responses=True,
            )
        except Exception:
            logging.warning("Redis can't be connected.")
        return self.REDIS
    
    def health(self):
        self.REDIS.ping()
        a, b = "xx", "yy"
        self.REDIS.set(a, b, 3)

        if self.REDIS.get(a) == b:
            return True
        
    def is_alive(self):
        return self.REDIS is not None
    
    def exist(self, k):
        if not self.REDIS:
            return
        try:
            return self.REDIS.exists(k)
        except Exception as e:
            logging.warning("RedisDB.exist " + str(k) + " got exception: " + str(e))
            self.__open__()

    def get(self, k):
        """
        参数: k — 要获取其值的键名。
        返回值: 对应键的值；若未建立连接或发生异常则可能返回None。
        功能: 获取指定键的值，出错时尝试重新连接。
        """
        if not self.REDIS:
            return
        try:
            return self.REDIS.get(k)
        except Exception as e:
            logging.warning("RedisDB.get " + str(k) + " got exception: " + str(e))
            self.__open__()

    def set_obj(self, k, obj, exp=3600):
        """
        参数:
            k — 要设置的键名。
            obj — 需要存储的对象（将被序列化为JSON字符串）。
            exp — 过期时间（秒），默认为3600秒。
        返回值: True表示成功设置；否则为False。
        功能: 将Python对象以JSON格式存入Redis，可指定过期时间。

        """
        try:
            self.REDIS.set(k, json.dumps(obj, ensure_ascii=False), exp)
            return True
        except Exception as e:
            logging.warning("RedisDB.set_obj " + str(k) + " got exception: " + str(e))
            self.__open__()
        return False
    
    def set(self, k, v, exp=3600):
        """
        参数:
            k — 要设置的键名。
            v — 要存储的值。
            exp — 过期时间（秒），默认为3600秒。
        返回值: True表示成功设置；否则为False。
        功能: 直接设置键值对，支持设置过期时间。
        """
        try:
            self.REDIS.set(k, v, exp)
            return True
        except Exception as e:
            logging.warning("RedisDB.set " + str(k) + " got exception: " + str(e))
            self.__open__()
        return False
    
    def sadd(self, key: str, member: str):
        """
        参数:
            key — 集合的键名。
            member — 要添加到集合中的元素。
        返回值: True表示添加成功；否则为False。
        功能: 向指定的集合中添加一个新成员。
        """
        try:
            self.REDIS.sadd(key, member)
            return True
        except Exception as e:
            logging.warning("RedisDB.sadd " + str(key) + " got exception: " + str(e))
            self.__open__()
        return False

    def srem(self, key: str, member: str):
        """
        参数:
            key — 集合的键名。
            member — 要从集合中移除的元素。
        返回值: True表示移除成功；否则为False。
        功能: 从指定的集合中删除指定元素。
        """
        try:
            self.REDIS.srem(key, member)
            return True
        except Exception as e:
            logging.warning("RedisDB.srem " + str(key) + " got exception: " + str(e))
            self.__open__()
        return False

    def smembers(self, key: str):
        """
        参数: key — 集合的键名。
        返回值: 包含所有成员的列表；若出错则返回None。
        功能: 获取指定集合的所有成员。
        """
        try:
            res = self.REDIS.smembers(key)
            return res
        except Exception as e:
            logging.warning(
                "RedisDB.smembers " + str(key) + " got exception: " + str(e)
            )
            self.__open__()
        return None

    def zadd(self, key: str, member: str, score: float):
        """
        参数:
            key — 有序集合的键名。
            member — 要添加的成员。
            score — 该成员的分数。
        返回值: True表示添加成功；否则为False。
        功能: 向有序集合中添加带有特定分数的成员。
        """
        try:
            self.REDIS.zadd(key, {member: score})
            return True
        except Exception as e:
            logging.warning("RedisDB.zadd " + str(key) + " got exception: " + str(e))
            self.__open__()
        return False

    def zcount(self, key: str, min: float, max: float):
        """
        参数:
            key — 有序集合的键名。
            min — 最小分数边界。
            max — 最大分数边界。
        返回值: 在给定范围内的元素数量；若出错则返回0。
        功能: 统计有序集合中分数介于[min, max]之间的元素个数。
        """
        try:
            res = self.REDIS.zcount(key, min, max)
            return res
        except Exception as e:
            logging.warning("RedisDB.zcount " + str(key) + " got exception: " + str(e))
            self.__open__()
        return 0

    def zpopmin(self, key: str, count: int):
        """
        参数:
            key — 有序集合的键名。
            count — 弹出最少元素的数目。
        返回值: 被弹出的元素及其分数组成的元组列表；若出错则返回None。
        功能: 弹出并返回有序集合中分数最小的若干个元素。
        """
        try:
            res = self.REDIS.zpopmin(key, count)
            return res
        except Exception as e:
            logging.warning("RedisDB.zpopmin " + str(key) + " got exception: " + str(e))
            self.__open__()
        return None

    def zrangebyscore(self, key: str, min: float, max: float):
        """
        参数:
            key — 有序集合的键名。
            min — 最小分数边界。
            max — 最大分数边界。
        返回值: 分数在[min, max]范围内的所有元素列表；若出错则返回None。
        功能: 获取有序集合中分数落在指定区间内的所有元素。
        """
        try:
            res = self.REDIS.zrangebyscore(key, min, max)
            return res
        except Exception as e:
            logging.warning(
                "RedisDB.zrangebyscore " + str(key) + " got exception: " + str(e)
            )
            self.__open__()
        return None

    def transaction(self, key, value, exp=3600):
        """
        参数:
            key — 事务涉及的键名。
            value — 要设置的新值。
            exp — 过期时间（秒），默认为3600秒。
        返回值: True表示事务执行成功；否则为False。
        功能: 使用管道技术实现原子性的SET操作（仅当键不存在时才设置）。
        """
        try:
            pipeline = self.REDIS.pipeline(transaction=True)
            pipeline.set(key, value, exp, nx=True)
            pipeline.execute()
            return True
        except Exception as e:
            logging.warning(
                "RedisDB.transaction " + str(key) + " got exception: " + str(e)
            )
            self.__open__()
        return False
    
    def queue_product(self, queue, message, exp=settings.SVR_QUEUE_RETENTION) -> bool:
        """
        参数:
            queue — 目标队列的名称。
            message — 要发送的消息内容。
            exp — 消息保留的时间长度，默认取自全局配置。
        返回值: True表示消息成功入队；否则为False。
        功能: 将消息作为JSON负载推送到指定的Redis流队列中，最多重试三次。
        """
        for _ in range(3):
            try:
                payload = {"message": json.dumps(message)}
                pipeline = self.REDIS.pipeline()
                pipeline.xadd(queue, payload)
                pipeline.execute()
                return True
            except Exception as e:
                logging.exception(
                    "RedisDB.queue_product " + str(queue) + " got exception: " + str(e)
                )
        return False
    
    def queue_consumer(self, queue_name, group_name, consumer_name, msg_id=b">") -> Payload:
        """
        参数:
            queue_name — 消费的目标队列名称。
            group_name — 消费者所属的消费组名称。
            consumer_name — 当前消费者的标识符。
            msg_id — 起始读取的消息ID，默认为">"。
        返回值: 一个Payload对象，代表从队列中取出的消息；如果没有新消息则返回None。
        功能: 从指定的消费组中读取一条消息，如果相应的消费组不存在则先创建它。
        """
        try:
            group_info = self.REDIS.xinfo_groups(queue_name)
            if not any(e["name"] == group_name for e in group_info):
                self.REDIS.xgroup_create(queue_name, group_name, id="0", mkstream=True)
            args = {
                "groupname": group_name,
                "consumername": consumer_name,
                "count": 1,
                "block": 10000,
                "streams": {queue_name: msg_id},
            }
            messages = self.REDIS.xreadgroup(**args)
            if not messages:
                return None
            stream, element_list = messages[0]
            msg_id, payload = element_list[0]
            res = Payload(self.REDIS, queue_name, group_name, msg_id, payload)
            return res
        except Exception as e:
            if "key" in str(e):
                pass
            else:
                logging.exception(
                    "RedisDB.queue_consumer "
                    + str(queue_name)
                    + " got exception: "
                    + str(e)
                )
        return None
    
    def get_unacked_for(self, consumer_name, queue_name, group_name):
        """
        参数:
            consumer_name — 消费者的标识符。
            queue_name — 队列名称。
            group_name — 消费组名称。
        返回值: 一个Payload对象，代表尚未确认的消息；若无此类消息则返回None。
        功能: 查找并返回指定消费者在指定队列中有但尚未确认的消息。
        """
        try:
            group_info = self.REDIS.xinfo_groups(queue_name)
            if not any(e["name"] == group_name for e in group_info):
                return
            pendings = self.REDIS.xpending_range(
                queue_name,
                group_name,
                min=0,
                max=10000000000000,
                count=1,
                consumername=consumer_name,
            )
            if not pendings:
                return
            msg_id = pendings[0]["message_id"]
            msg = self.REDIS.xrange(queue_name, min=msg_id, count=1)
            _, payload = msg[0]
            return Payload(self.REDIS, queue_name, group_name, msg_id, payload)
        except Exception as e:
            if "key" in str(e):
                return
            logging.exception(
                "RedisDB.get_unacked_for " + consumer_name + " got exception: " + str(e)
            )
            self.__open__()

    def queue_info(self, queue, group_name) -> dict | None:
        """
        参数:
            queue — 查询信息的队列名称。
            group_name — 感兴趣的消费组名称。
        返回值: 包含有关指定消费组详细信息的字典；若找不到匹配项或出错则返回None。
        功能: 获取关于特定消费组的状态信息。
        """
        try:
            groups = self.REDIS.xinfo_groups(queue)
            for group in groups:
                if group["name"] == group_name:
                    return group
        except Exception as e:
            logging.warning(
                "RedisDB.queue_info " + str(queue) + " got exception: " + str(e)
            )
        return None

REDIS_CONN = RedisDB()

class RedisDistributedLock:
    """
    使用Redis实现分布式锁的类。
    """

    def __init__(self, lock_key, timeout=10):
        self.lock_key = lock_key
        self.lock_value = str(uuid.uuid4())
        self.timeout = timeout

    @staticmethod
    def clear_lock(lock_key):
        REDIS_CONN.REDIS.delete(lock_key)

    def acquire_lock(self):
        end_time = time.time() + self.timeout
        while time.time() < end_time:
            if REDIS_CONN.REDIS.setnx(self.lock_key, self.lock_value):
                return True
            time.sleep(1)
        return False
    
    def release_lock(self):
        if REDIS_CONN.REDIS.get(self.lock_key) == self.lock_value:
            REDIS_CONN.REDIS.delete(self.lock_key)

    def __enter__(self):
        self.acquire_lock()

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.release_lock()