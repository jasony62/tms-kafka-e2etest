#!/usr/bin/env python

import sys
import os
import getopt
import threading
import json
import time
import logging
import logging.config
from kafka import KafkaProducer, KafkaConsumer, TopicPartition, ConsumerRebalanceListener
from jsonschema import Draft7Validator
from jsonpath_ng import parse

logging.config.fileConfig('conf/logging.conf')
logger = logging.getLogger()
loggerResult = logging.getLogger('result')  # 记录失败信息


# 将json对象变为path-value对应关系
def _flatJson(json, keys=(), flatted=None):
    flatted = flatted or {}
    if isinstance(json, dict):  # 对象
        for key, value in json.items():
            _flatJson(value, keys + (key, ), flatted)
    elif isinstance(json, (list, tuple)):  # 数组
        for index in range(len(json)):
            _flatJson(json[index], flatted, keys + (index, ), flatted)
    else:
        flatted[keys] = json

    return flatted


# 通过path方式访问被包裹json对象
class _JsonWrap:
    def __init__(self, file=None, data=None):
        if (file is not None):
            with open(file) as f:
                jsonstr = f.read()
            self.data = json.loads(jsonstr)
        else:
            self.data = data

    def get(self, path):
        values = parse(path).find(self.data)
        if len(values) == 1:
            return values[0].value
        elif len(values) > 1:
            logger.warning("路径'{}'在对象中配置多个值\n{}".format(path, self.data))

        return None


# 指定的测试运行需要的数据
class _TestContext:
    def __init__(self, rawArgs):
        self.rawArgs = rawArgs
        # 测试开始时间
        self.startAtStr = time.strftime('%Y%m%d-%H%M%S', time.localtime())
        # 测试名称
        self.name = rawArgs.name or self.startAtStr
        # 获取连接kafka参数
        config = _JsonWrap(file=rawArgs.config)
        self.servers = config.get('kafka.servers')
        self.produce_topic_name = config.get('kafka.produce.topic_name')
        self.produce_client_id = config.get('kafka.produce.client_id')
        self.produce_partition_seq = config.get('kafka.produce.partition')
        self.consume_topic_name = config.get('kafka.consume.topic_name')
        self.group_id = config.get('kafka.consume.group_id')
        self.consume_client_id = config.get('kafka.consume.client_id')
        self.consume_partition_id = config.get('kafka.consume.partition')
        # 设置输出测试结果的logger
        self.loggerHandler = None
        self._setResultLogger()
        # 准备测试实例
        self.testIns = _TestIns(self)

    def __del__(self):
        self.release()

    # 设置输出测试结果的logger
    def _setResultLogger(self):
        resultPath = os.path.abspath(
            self.rawArgs.result or 'result/{}.log'.format(self.startAtStr))
        resultDir = os.path.dirname(resultPath)
        if not os.path.exists(resultDir):
            os.makedirs(resultDir)

        self.loggerHandler = fh = logging.FileHandler(resultPath)
        loggerResult.addHandler(fh)

    def release(self):
        loggerResult.removeHandler(self.loggerHandler)
        self.loggerHandler = None

    def outputResult(self):
        rawArgs = self.rawArgs
        loggerResult.info('*' * 60)
        loggerResult.info('name={}'.format(self.name))
        loggerResult.info('start={}'.format(self.startAtStr))
        loggerResult.info('produce-data={}'.format(rawArgs.data))
        loggerResult.info('consume-schema={}'.format(rawArgs.schema))
        loggerResult.info('consume-key={}'.format(rawArgs.key))
        # 输出执行
        self.testIns.outputResult()
        # 双划线作为结束
        loggerResult.info('=' * 60)


# 用户提供的测试参数
class TestArgs:
    def __init__(self,
                 config=None,
                 data=None,
                 schema=None,
                 key=None,
                 result=None,
                 name=None,
                 logLevel=None,
                 maxretries=None):
        self.config = config
        self.data = data
        self.schema = schema
        self.key = key
        self.result = result
        self.name = name
        self.logLevel = logLevel
        self.maxretries = maxretries


# 测试实例
class _TestIns:
    def __init__(self, context):
        rawArgs = context.rawArgs
        self.context = context
        self.__validated = []  # 要进行校验的数据
        self.__errmsgs = []  # 每条记录对应一个错误列表
        # 用于筛选消费记录的数据
        key_json = None
        if rawArgs.key is not None:
            with open(rawArgs.key) as f:
                jsonstr = f.read()
                key_json = json.loads(jsonstr)
        self.keyJson = key_json

    # 检查json对象是否和指定的key匹配
    def matchWithKey(self, data):
        passed = True
        keyData = _flatJson(self.keyJson)
        for keys, value in keyData.items():
            path = ''.join(['[{}]'.format(k) for k in keys])
            jp_expr = parse(path)
            # 有一个不匹配就整体失败
            if len([m for m in jp_expr.find(data) if value == m.value]) != 1:
                passed = False
                break

        return passed

    # 校验数据
    def validate(self, data, schema):
        logger.debug('需要校验的数据\n{}'.format(data))
        if self.keyJson is not None:
            logger.debug('检查数据是否和指定的key匹配，指定的key为\n{}'.format(self.keyJson))
            if not self.matchWithKey(data):
                logger.warning('获得的数据和key不匹配，跳过校验')
                return

        logger.debug('开始数据校验')
        errmsgs = []  # 一条记录的校验结果
        v = Draft7Validator(schema)
        for error in v.iter_errors(data):
            errmsgs.append(error.message)
            logger.warning('发现校验错误：{}'.format(error.message))
        logger.info('完成1次数据校验，有{}个错误'.format(len(errmsgs)))
        # 记录执行结果
        self.__validated.append(data)
        self.__errmsgs.append(errmsgs)

    # 输出校验结果
    def outputResult(self):
        if len(self.__validated):
            for i in range(len(self.__validated)):
                # 输出被校验的数据
                data = self.__validated[i]
                loggerResult.info('-' * 60)  # 单划线作为分割线
                loggerResult.info(data)
                # 输出校验错误信息
                errmsgs = self.__errmsgs[i]
                if len(errmsgs):
                    loggerResult.info('<' * 60)
                    loggerResult.info('errors={}'.format(len(errmsgs)))
                    loggerResult.info('>' * 60)
                    for errmsg in errmsgs:
                        loggerResult.info(errmsg)


# 监听Rebalance事件
class _TmsConsumerRebalanceListener(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        logger.debug("ConsumerRebalance - 消费端取消分区\n{}".format(revoked))

    def on_partitions_assigned(self, assigned):
        logger.debug("ConsumerRebalance - 消费端分配分区\n{}".format(assigned))


# 消费线程
class __ConsumeThread(threading.Thread):
    def __init__(self, threadID, name, context):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.context = context
        self.__stopped = False  # 终止执行

    def run(self):
        logger.debug("开始消费线程：" + self.name)
        self.consume()
        logger.debug("退出消费线程：" + self.name)

    def stop(self):
        self.__stopped = True

    # 消费数据
    def consume(self):
        context = self.context
        # 读取用于校验数据的schema
        with open(context.rawArgs.schema) as f:
            jsonstr = f.read()
            schema = json.loads(jsonstr)

        if context.consume_partition_id is None:
            # 自动分配分区
            logger.debug("等待消费端自动分配分区")
            consumer = KafkaConsumer(group_id=context.group_id,
                                     client_id=context.consume_client_id,
                                     enable_auto_commit=False,
                                     bootstrap_servers=context.servers)
            rebalanceListener = _TmsConsumerRebalanceListener()
            consumer.subscribe([context.consume_topic_name],
                               listener=rebalanceListener)
            ids = consumer.partitions_for_topic(context.consume_topic_name)
            logger.debug("消费端获得分区 partition={}".format(ids))
        else:
            # 指定分区
            logger.debug("消费端指定分区 partition={}".format(
                context.consume_partition_id))
            consumer = KafkaConsumer(group_id=context.group_id,
                                     client_id=context.consume_client_id,
                                     enable_auto_commit=False,
                                     bootstrap_servers=context.servers)
            part = TopicPartition(context.consume_topic_name,
                                  context.consume_partition_id)
            consumer.assign([part])

        logger.info(
            '消费端【group_id={},client_id={}】bootstrap_connected={} 开始从【topic={}】接收数据'
            .format(context.group_id, context.consume_client_id,
                    consumer.bootstrap_connected(),
                    context.consume_topic_name))

        max_loop = context.rawArgs.maxretries or 10  # 最大循环次数
        loop = 0  # 已循环次数
        # 从队列中读取数据
        while not self.__stopped and loop < max_loop:
            loop += 1
            batch = consumer.poll(timeout_ms=100)  # 从kafka获取批量数据
            if len(batch.values()) == 0:
                logger.debug('[{}] - 未拉取到数据'.format(loop))
                time.sleep(1)  # 隔1秒钟取1次
                continue
            logger.info('[{}] - 拉取到数据'.format(loop))
            # 如果一次拉取获得多条数据应该如何理解？
            for records in batch.values():
                for record in records:
                    logger.debug("消费端接收到数据\n{}".format(record))
                    data = json.loads(record.value)
                    context.testIns.validate(data, schema)

            # 提交一个批次的数据
            consumer.commit()
            # 获得记录，退出，不再消费
            break

        consumer.close()


# 生产数据
def __produce(context):
    producer = KafkaProducer(client_id=context.produce_client_id,
                             bootstrap_servers=context.servers,
                             linger_ms=5)
    # 读取要发送的数据
    with open(context.rawArgs.data) as f:
        jsonstr = f.read()

    # 字符串转字节数组
    future = producer.send(context.produce_topic_name,
                           value=jsonstr.encode(),
                           partition=context.produce_partition_seq)
    result = future.get(timeout=3)
    logger.info("完成发送数据 topic={}, partition={}, offset={}".format(
        result.topic, result.partition, result.offset))
    logger.debug('生产端完成数据发送\n{}'.format(result))

    producer.close()


# 运行一次测试
def _run(rawArgs):
    if rawArgs.config is None:
        logger.critical('必须指定测试配置文件')
        sys.exit(0)

    # 设置控制台日志输出级别
    rawArgs.logLevel and logger.setLevel(rawArgs.logLevel)

    context = _TestContext(rawArgs)

    if rawArgs.data is not None:
        # 生产数据
        __produce(context)

    if rawArgs.schema is not None:
        # 创建消费线程
        consumeThread = __ConsumeThread(1, "Consume_Thread-1", context)
        # 开启新线程
        consumeThread.start()
        # 等待消费线程结束
        try:
            consumeThread.join()
        except KeyboardInterrupt:
            logger.info('请耐心等待程序自动结束')
            consumeThread.stop()
            consumeThread.join()

    # 输出测试结果
    context.outputResult()
    context.release()


# 通过命令行启动运行，解析传入的参数
def __main(argv):
    try:
        opts, args = getopt.getopt(argv, "hc:d:s:k:r:n:b:", [
            "config=", "data=", "schema=", "key=", "result=", "name=",
            "loglevel=", "maxretries="
        ])
    except getopt.GetoptError:
        print(
            'tester.py -c <test config> -d <produce data file> -s <consume schema file>'
        )
        sys.exit(2)

    batch_file = None  # 批量定义
    # 初始化命令行参数
    rawArgs = TestArgs()
    for opt, arg in opts:
        if opt == '-h':
            print(
                'tester.py -c <test config> -d <produce data file> -s <consume schema file>'
            )
            sys.exit(0)
        elif opt in ("-c", "--config"):
            rawArgs.config = arg
        elif opt in ("-d", "--data"):
            rawArgs.data = arg
        elif opt in ("-s", "--schema"):
            rawArgs.schema = arg
        elif opt in ("-k", "--key"):
            rawArgs.key = arg
        elif opt in ("-r", "--result"):
            rawArgs.result = arg
        elif opt in ("-n", "--result"):
            rawArgs.name = arg
        elif opt in ("--loglevel"):
            rawArgs.logLevel = arg
        elif opt in ("--maxretries"):
            rawArgs.maxretries = int(arg)
        elif opt in ("-b", "--batch"):
            batch_file = arg

    # 单次执行
    if batch_file is None:
        _run(rawArgs)
    else:
        with open(batch_file) as f:
            batchstr = f.read()
            batchjson = json.loads(batchstr)
        if not isinstance(batchjson, (list, tuple)):
            logger.error("通过'-b'或‘--batch’指定的批量执行文件内容不是JSON数组")
        else:
            for single in batchjson:
                singleWrap = _JsonWrap(data=single)
                singleRawArgs = TestArgs()
                singleRawArgs.logLevel = rawArgs.logLevel
                singleRawArgs.config = singleWrap.get(
                    'config') or rawArgs.config
                singleRawArgs.data = singleWrap.get('data') or rawArgs.data
                singleRawArgs.schema = singleWrap.get(
                    'schema') or rawArgs.schema
                singleRawArgs.key = singleWrap.get('key') or rawArgs.key
                singleRawArgs.result = singleWrap.get(
                    'result') or rawArgs.result
                singleRawArgs.name = singleWrap.get('name') or rawArgs.name
                _run(singleRawArgs)

    # 退出
    logger.info('完成测试，退出程序 ^_^')


if __name__ == "__main__":
    __main(sys.argv[1:])
