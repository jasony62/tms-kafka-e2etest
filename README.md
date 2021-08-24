搭建测试消息驱动微服务框架的工具。

# 配置文件

必须存在`conf/logging.conf`用于设置执行结果输出。

# 提供测试数据

程序支持通过命令行参数指定测试数据文件。

| 数据文件            | 说明                                                                                  | 指定参数 |
| ------------------- | ------------------------------------------------------------------------------------- | -------- |
| test-config.json    | 指定测试时的配置参数，例如连接 kafka 相关参数。                                       | -c       |
| produce-data.json   | 生产端要发送的数据，json 格式。                                                       | -d       |
| consume-schema.json | 消费端接收到数据后，进行数据校验的 schema 数据，json 格式。                           | -s       |
| consume-key.json    | 消费端接收到收据后，当接收数据中的字段与指定文件中的数据一致时才进行校验，json 格式。 | -k       |
| batch.json          | 批量测试文件。                                                                        | -b       |

## 配置参数

| 字段                     | 说明                       | 必填 | 类型          |
| ------------------------ | -------------------------- | ---- | ------------- |
| kafka                    | 连接 kafka 参数            | 是   | object        |
| kafka.servers            | kafka 连接地址             | 是   | string\|array |
| kafka.produce            | kafka 生产端配置           | 是   | object        |
| kafka.produce.topic_name | kafka 生产端主题名称       | 是   | string        |
| kafka.produce.client_id  | kafka 生产端客户端名称     | 是   | string        |
| kafka.consume            | kafka 消费端配置           | 是   | object        |
| kafka.consume.topic_name | kafka 消费端主题名称       | 是   | string        |
| kafka.consume.group_id   | kafka 消费端主题消费组名称 | 是   | string        |
| kafka.consume.client_id  | kafka 消费端客户端名称     | 是   | string        |

参见 example 目录下的示例。

# 启动 kafka

需要在项目下建立`kafka-logs-1`文件夹（和`docker-compose.yml`文件中的内容一致）。

在本地启动 kafka 实例，便于验证测试数据。

> docker compose up zookeeper broker1

# 构建测试镜像

> docker build -f Dockerfile-tester -t tms/kafka-tester .

或者

> docker compose build tester

# 进入容器

> docker run -it --rm --name tms-kafka-tester --network try-kafka-test_kf_net -v $PWD/example:/home/tests tms/kafka-tester sh

测试结果输出到宿主机

> docker run -it --rm --name tms-kafka-tester --network try-kafka-test_kf_net -v $PWD/example:/home/tests -v $PWD/result:/home/result tms/kafka-tester sh

便于在本地环境上直接修改代码和日志输出配置

> docker run -it --rm --name tms-kafka-tester --network try-kafka-test_kf_net -v $PWD/example:/home/tests -v $PWD/src/tester.py:/home/tester.py -v $PWD/src/logging.conf:/home/conf/logging.conf tms/kafka-tester sh

# 执行测试程序

`-c`或`--config`参数指定测试基本信息，例如：测试 kafka 连接参数等。

`-d`或`--data`参数指定生产端数据文件。如果不指定，就不生产数据。

`-s`或`--schema`参数指定消费端 schema 文件。如果不指定就不消费数据。

`-k`或`--key`参数指定消息端要进行校验的数据的主键。该参数是为了解决队列中有垃圾数据干扰测试结果的问题。

`-r`或`--result`参数指定校验结果输出文件。如果不指定，默认在当前文件夹下按`result/YYYYMMDD-HHMMSS.log`生成文件。

`-n`或`--name`参数指定测试用例的名称，将输出到校验结果文件中。

`--loglevel`参数指定控制台日志输出级别，必须是`INFO`，`DEBUG`等字符串。

`--maxretries`参数指定尝试获取消费数据的最大次数，默认 10 次。

`-b`或`--batch`参数指定批量执行的测试。

## 单次执行

每隔 1 秒钟消费端拉取 1 次数据，最多拉取`maxretries`次，只要拉取到数据就结束，不论数据是否和`key`匹配。

> python3 tester.py -c tests/test-config.json -d tests/produce-data.json -s tests/consume-schema.json -k tests/consume-key.json -n test001 -r result/test001.log --loglevel DEBUG

## 批量执行

> python3 tester.py -b tests/batch.json --loglevel DEBUG

可以通过命令行之行文件中未指定的参数。如果文件指定和命令行同时指定，使用文件中指定的值。

> python3 tester.py -b tests/batch2.json -r result/batch2.log

| 字段名称 | 说明                                                                                  |
| -------- | ------------------------------------------------------------------------------------- |
| config   | 指定测试时的配置参数，例如连接 kafka 相关参数。                                       |
| data     | 生产端要发送的数据，json 格式。                                                       |
| schema   | 消费端接收到数据后，进行数据校验的 schema 数据，json 格式。                           |
| key      | 消费端接收到收据后，当接收数据中的字段与指定文件中的数据一致时才进行校验，json 格式。 |
| result   | 记录测试结果的文件。                                                                  |
| name     | 测试名称。                                                                            |

# kafka 命令

进入 kafka 容器

> docker exec -it try-kafka_broker1 /bin/bash

查看消费组消费情况

> kafka-consumer-groups.sh --bootstrap-server broker1:9092 --describe --group test_group

清空指定 topic 中的数据

> kafka-topics.sh --bootstrap-server broker1:9092 --delete --topic test_topic

查看 topic 中的数据

> kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic test_topic --from-beginning

> kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic test_topic --offset 0 --partition 0

# 参考

https://json-schema.org

https://github.com/Julian/jsonschema

https://python-jsonschema.readthedocs.io/en/stable/

https://docs.python.org/3/howto/logging.html
