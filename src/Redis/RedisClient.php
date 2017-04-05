<?php

namespace Comoyi\Redis;

use Comoyi\Redis\RedisSentinel;
use Comoyi\Redis\RedisMaster;
use Comoyi\Redis\RedisSlave;
use Exception;

/**
 * redis操作类
 */
class RedisClient {

    /**
     * 配置
     *
     * @var array
     */
    protected $configs = [];

    /**
     * 用于执行redis操作的池
     *
     * @var array
     */
    protected $pool = [];

    /**
     * 当前选择的db
     *
     * @var int
     */
    protected $currentDb = 0;

    /**
     * 哨兵对象
     *
     * @var \Comoyi\Redis\RedisSentinel
     */
    protected $sentinel = null;

    /**
     * 要操作的master名称
     *
     * @var string
     */
    protected $masterName = '';

    /**
     * 构造函数
     *
     * @param array $config
     */
    public function __construct($config = []) {

        $defaultConfig = [
            'type' => 'direct', // direct: 直连, sentinel: 由sentinel决定host与port
            'password' => 'redispassword', // redis auth 密码
            'master_name' => 'mymaster', // master name
            'direct' => [
                'masters' => [
                    [
                        'host' => '127.0.0.1',
                        'port' => '6379',
                    ],
                ],
                'slaves' => [
                    [
                        'host' => '127.0.0.1',
                        'port' => '6381',
                    ],
                    [
                        'host' => '127.0.0.1',
                        'port' => '6382',
                    ],
                ],
            ],
            'sentinel' => [
                'sentinels' => [
                    [
                        'host' => '127.0.0.1',
                        'port' => '5000',
                    ],
                    [
                        'host' => '127.0.0.1',
                        'port' => '5001',
                    ],
                ],
            ],
        ];

        $this->setConfig($defaultConfig);
        $this->setConfig($config);

        if('sentinel' === $this->configs['type']) { // sentinel方式
            $this->masterName = $this->configs['master_name'];

            $this->sentinel = new RedisSentinel(); //创建sentinel

            // 根据配置添加sentinel
            foreach ($this->configs['sentinel']['sentinels'] as $s) {
                $this->sentinel->addNode($s['host'], $s['port']);
            }
        }

    }

    /**
     * 获取master配置
     */
    protected function getMasterConfigs() {
        if('sentinel' === $this->configs['type']) {
            return $this->getMasterConfigsBySentinel();
        }
        $randomMaster = rand(0, (count($this->configs['direct']['masters']) - 1)); // 随机取一个master的配置
        $config = [
            'host' => $this->configs['direct']['masters'][$randomMaster]['host'],
            'port' => $this->configs['direct']['masters'][$randomMaster]['port'],
            'password' => $this->configs['password'],
        ];
        return $config;
    }

    /**
     * 获取slave配置
     */
    protected function getSlaveConfigs() {
        if('sentinel' === $this->configs['type']) {
            return $this->getSlaveConfigsBySentinel();
        }
        if(0 === count($this->configs['direct']['slaves'])) { // 没有slave则取master
            return $this->getMasterConfigs();
        }
        $randomSlave = rand(0, (count($this->configs['direct']['slaves']) - 1)); // 随机取一个slave的配置
        $config = [
            'host' => $this->configs['direct']['slaves'][$randomSlave]['host'],
            'port' => $this->configs['direct']['slaves'][$randomSlave]['port'],
            'password' => $this->configs['password'],
        ];
        return $config;
    }

    /**
     * 通过sentinel获取master配置
     */
    protected function getMasterConfigsBySentinel() {
        $masters = $this->sentinel->getMasters($this->masterName);
        $config = [
            'host' => $masters[0],
            'port' => $masters[1],
            'password' => $this->configs['password'],
        ];
        return $config;
    }

    /**
     * 通过sentinel获取slave配置
     */
    protected function getSlaveConfigsBySentinel() {
        $slaves = $this->sentinel->getSlaves($this->masterName);
        if(0 === count($slaves)) { // 没有slave则取master
            return $this->getMasterConfigsBySentinel();
        }
        $random = rand(0, (count($slaves) - 1)); // 随机取一个slave的配置
        $config = [
            'host' => $slaves[$random]['ip'],
            'port' => $slaves[$random]['port'],
            'password' => $this->configs['password'],
        ];
        return $config;
    }

    /**
     * 设置配置
     *
     * @param array $config
     */
    protected function setConfig($config) {
        $this->configs = array_merge($this->configs, $config);
    }

    /**
     * 判断只读还是读写
     *
     * @param string $command
     * @return string
     */
    protected function judge($command) {
        $masterOrSlave = 'master';

        // 只读的操作
        $readOnlyCommands = [
            'get',
            'hGet',
            'hMGet',
            'hGetAll',
            'sMembers',
            'zRange',
            'exists',
        ];

        if (in_array($command, $readOnlyCommands)) {
             $masterOrSlave = 'slave';
        }
        return $masterOrSlave;
    }

    /**
     * 获取连接
     *
     * @param string $masterOrSlave [master / slave]
     * @return mixed
     * @throws Exception
     */
    protected function getHandler($masterOrSlave) {
        if (!isset($this->pool[$masterOrSlave]) || is_null($this->pool[$masterOrSlave])) {
            // 创建
            switch($masterOrSlave) {
                case 'master':
                    $redis = new RedisMaster($this->getMasterConfigs());
                    break;
                case 'slave':
                    $redis = new RedisSlave($this->getSlaveConfigs());
                    break;
                default:
                    throw new Exception('must be master or slave');
            }
            $this->pool[$masterOrSlave] = $redis;
            $handler = $this->pool[$masterOrSlave]->getHandler();

            // 如果当前不在默认库则切换到指定库
            if (0 != $this->currentDb) {
                $handler->select($this->currentDb);
            }

        }
        return $this->pool[$masterOrSlave]->getHandler();
    }

    /**
     * 切换database
     *
     * @param int $index db索引
     */
    public function select($index = 0) {

        if (isset($this->pool['master']) && !is_null($this->pool['master'])) {
            $this->pool['master']->getHandler()->select($index);
        }

        if (isset($this->pool['slave']) && !is_null($this->pool['slave'])) {
            $this->pool['slave']->getHandler()->select($index);
        }

        // 记录当前redis db
        $this->currentDb = $index;
    }

    /**
     * 执行lua脚本
     *
     * eval是PHP关键字PHP7以下不能作为方法名
     *
     * @param string $script 脚本代码
     * @param array $args 传给脚本的KEYS, ARGV组成的索引数组（不是key-value对应，是先KEYS再ARGV的索引数组，KEYS, ARGV数量可以不同） 例：['key1', 'key2', 'argv1', 'argv2', 'argv3']
     * @param int $quantity 传给脚本的KEY数量
     * @return mixed
     */
    public function evaluate ($script, $args, $quantity) {
        return $this->getHandler($this->judge(__FUNCTION__))->eval($script, $args, $quantity);
    }

    /**
     * 根据脚本sha1执行对应lua脚本
     *
     * Evaluates a script cached on the server side by its SHA1 digest.
     *
     * @param string $scriptSha 脚本代码sha1值
     * @param array $args 传给脚本的KEYS, ARGV组成的索引数组（不是key-value对应，是先KEYS再ARGV的索引数组，KEYS, ARGV数量可以不同） 例：['key1', 'key2', 'argv1', 'argv2', 'argv3']
     * @param int $quantity 传给脚本的KEY数量
     * @return mixed
     */
    public function evalSha($scriptSha, $args, $quantity) {
        return $this->getHandler($this->judge(__FUNCTION__))->evalSha($scriptSha, $args, $quantity);
    }

    /**
     * 执行script
     *
     * @param string $command
     * @param string $script
     * @return mixed
     */
    public function script($command, $script) {
        return $this->getHandler($this->judge(__FUNCTION__))->script($command, $script);
    }

    /**
     * 根据正则获取key
     *
     * @param $pattern
     * @return mixed
     */
    public function keys($pattern) {
        return $this->getHandler($this->judge(__FUNCTION__))->keys($pattern);
    }

    /**
     * 获取key对应的值
     *
     * @param string $key key
     * @return mixed
     */
    public function get($key) {
        return $this->getHandler($this->judge(__FUNCTION__))->get($key);
    }

    /**
     * 设置key - value
     *
     * set('key', 'value');
     * set('key', 'value', ['nx']);
     * set('key', 'value', ['xx']);
     * set('key', 'value', ['ex' => 10]);
     * set('key', 'value', ['px' => 1000]);
     * set('key', 'value', ['nx', 'ex' => 10]);
     * set('key', 'value', ['nx', 'px' => 1000]);
     * set('key', 'value', ['xx', 'ex' => 10]);
     * set('key', 'value', ['xx', 'px' => 1000]);
     *
     * @param string $key key
     * @param string $value value
     * @param array $opt 可选参数  可选参数可以自由组合 nx: key不存在时有效, xx: key存在时有效, ex: ttl[单位：s], px: ttl[单位：ms]
     * @return mixed
     */
    public function set($key, $value, $opt = null) {
        return $this->getHandler($this->judge(__FUNCTION__))->set($key, $value, $opt);
    }

    /**
     * 设置key - value同时设置剩余有效期
     *
     * 不建议使用
     * 请使用set方式代替
     *
     * Note: Since the SET command options can replace SETNX, SETEX, PSETEX,
     * it is possible that in future versions of Redis these three commands will be deprecated and finally removed.
     *
     * @param string $key key
     * @param int $seconds 剩余有效期 （单位：s / 秒）
     * @param string $value
     * @return mixed
     */
    public function setEx($key, $seconds, $value) {
        return $this->getHandler($this->judge(__FUNCTION__))->setEx($key, $seconds, $value);
    }

    /**
     * 设置key - value （仅在当前key不存在时有效）
     *
     * 不建议使用
     * 请使用set方式代替
     *
     * Note: Since the SET command options can replace SETNX, SETEX, PSETEX,
     * it is possible that in future versions of Redis these three commands will be deprecated and finally removed.
     *
     * @param string $key key
     * @param string $value value
     * @return mixed
     */
    public function setNx($key, $value) {
        return $this->getHandler($this->judge(__FUNCTION__))->setNx($key, $value);
    }

    /**
     * 设置生存时长
     *
     * @param string $key key
     * @param int $seconds 生存时长（单位：秒）
     * @return mixed
     */
    public function expire($key, $seconds) {
        return $this->getHandler($this->judge(__FUNCTION__))->expire($key, $seconds);
    }

    /**
     * 设置生存时长 - 毫秒级
     *
     * @param string $key
     * @param int $milliseconds 生存时长（单位：毫秒）
     * @return mixed
     */
    public function pExpire($key, $milliseconds) {
        return $this->getHandler($this->judge(__FUNCTION__))->pExpire($key, $milliseconds);
    }

    /**
     * 设置生存截止时间戳
     *
     * @param string $key key
     * @param int $timestamp 生存截止时间戳
     * @return mixed
     */
    public function expireAt($key, $timestamp) {
        return $this->getHandler($this->judge(__FUNCTION__))->expireAt($key, $timestamp);
    }

    /**
     * 设置生存截止时间戳 - 毫秒极
     *
     * @param string $key key
     * @param int $millisecondsTimestamp 生存截止时间戳（单位：毫秒）
     * @return mixed
     */
    public function pExpireAt($key, $millisecondsTimestamp) {
        return $this->getHandler($this->judge(__FUNCTION__))->pExpireAt($key, $millisecondsTimestamp);
    }

    /**
     * 获取key的生存时间
     *
     * @param string $key
     * @return mixed
     */
    public function ttl($key) {
        return $this->getHandler($this->judge(__FUNCTION__))->ttl($key);
    }

    /**
     * 删除key
     *
     * @param string $key key
     * @return mixed
     */
    public function del($key) {
        return $this->getHandler($this->judge(__FUNCTION__))->del($key);
    }

    /**
     * 判断key是否存在
     *
     * @param string $key
     * @return mixed
     */
    public function exists($key) {
        return $this->getHandler($this->judge(__FUNCTION__))->exists($key);
    }

    /**
     * 发布消息到指定频道
     * @param string $channel 频道
     * @param string $message 消息内容
     * @return mixed
     */
    public function publish($channel, $message) {
        return $this->getHandler($this->judge(__FUNCTION__))->publish($channel, $message);
    }

    /**
     * 自增 - 增幅为1
     *
     * @param string $key key
     * @return mixed
     */
    public function incr($key) {
        return $this->getHandler($this->judge(__FUNCTION__))->incr($key);
    }

    /**
     * 自增 - 增幅为指定值
     *
     * @param string $key key
     * @param int $increment 增量
     * @return mixed
     */
    public function incrBy($key, $increment) {
        return $this->getHandler($this->judge(__FUNCTION__))->incrBy($key, $increment);
    }

    /**
     * 自减 - 减幅为1
     *
     * @param string $key key
     * @return mixed
     */
    public function decr($key) {
        return $this->getHandler($this->judge(__FUNCTION__))->decr($key);
    }

    /**
     * 自减 - 减幅为指定值
     *
     * @param string $key key
     * @param int $decrement 减量
     * @return mixed
     */
    public function decrBy($key, $decrement) {
        return $this->getHandler($this->judge(__FUNCTION__))->decrBy($key, $decrement);
    }

    /**
     * 获取hash一个指定field的值
     *
     * @param string $key key
     * @param string $field
     * @return mixed
     */
    public function hGet($key, $field) {
        return $this->getHandler($this->judge(__FUNCTION__))->hGet($key, $field);
    }

    /**
     * 获取hash多个指定field
     *
     * @param string $key key
     * @param array $array field索引数组
     * @return mixed
     */
    public function hMGet($key, $array) {
        return $this->getHandler($this->judge(__FUNCTION__))->hMGet($key, $array);
    }

    /**
     * 获取整个hash的值
     *
     * @param string $key key
     * @return mixed
     */
    public function hGetAll($key) {
        return $this->getHandler($this->judge(__FUNCTION__))->hGetAll($key);
    }

    /**
     * 设置hash一个field
     *
     * @param string $key key
     * @param string $field
     * @param string $value
     * @return mixed
     */
    public function hSet($key, $field, $value) {
        return $this->getHandler($this->judge(__FUNCTION__))->hSet($key, $field, $value);
    }

    /**
     * 设置hash多个field
     *
     * @param string $key key
     * @param array $array 要设置的hash field 例：['field1' => 'value1', 'field2' => 'value2']
     * @return mixed
     */
    public function hMSet($key, $array) {
        return $this->getHandler($this->judge(__FUNCTION__))->hMSet($key, $array);
    }

    /**
     * hash中是否存在指定field
     *
     * 1 if the hash contains field.
     * 0 if the hash does not contain field, or key does not exist.
     *
     * @param string $key key
     * @param string $field
     * @return mixed
     */
    public function hExists($key, $field) {
        return $this->getHandler($this->judge(__FUNCTION__))->hExists($key, $field);
    }

    /**
     * 获取hash所有field
     *
     * @param string $key
     * @return mixed
     */
    public function hKeys($key) {
        return $this->getHandler($this->judge(__FUNCTION__))->hKeys($key);
    }


    /**
     * 移除hash中指定field
     *
     * @param string $key key
     * @param string $field
     * @return mixed
     */
    public function hDel($key, $field) {
        return $this->getHandler($this->judge(__FUNCTION__))->hDel($key, $field);
    }

    /**
     * 添加到队列头
     *
     * @param string $key key
     * @param string $value value
     * @return mixed
     */
    public function lPush($key, $value) {
        return $this->getHandler($this->judge(__FUNCTION__))->lPush($key, $value);
    }

    /**
     * 添加到队列尾
     *
     * @param string $key key
     * @param string $value value
     * @return mixed
     */
    public function rPush($key, $value) {
        return $this->getHandler($this->judge(__FUNCTION__))->rPush($key, $value);
    }

    /**
     * 删除并返回列表中第一个元素
     *
     * @param string $key
     * @return mixed
     */
    public function lPop($key) {
        return $this->getHandler($this->judge(__FUNCTION__))->lPop($key);
    }

    /**
     * 删除并返回列表中最后一个元素
     *
     * @param string $key
     * @return mixed
     */
    public function rPop($key) {
        return $this->getHandler($this->judge(__FUNCTION__))->rPop($key);
    }

    /**
     * 从队列中取出指定范围内的元素
     *
     * start stop 从0开始 - The offsets start and stop are zero-based indexes, with 0 being the first element of the list
     * These offsets can also be negative numbers indicating offsets starting at the end of the list. For example, -1 is the last element of the list, -2 the penultimate, and so on.
     *
     * @param string $key
     * @param int $start 起始
     * @param int $stop 截止
     * @return mixed
     */
    public function lRange($key, $start, $stop) {
        return $this->getHandler($this->judge(__FUNCTION__))->lRange($key, $start, $stop);
    }

    /**
     * 对列表进行修剪
     *
     * @param string $key
     * @param int $start 起始
     * @param int $stop 截止
     * @return mixed
     */
    public function lTrim($key, $start, $stop) {
        return $this->getHandler($this->judge(__FUNCTION__))->lTrim($key, $start, $stop);
    }

    /**
     * 获取列表长度
     *
     * @param string $key
     * @return mixed
     */
    public function lLen($key) {
        return $this->getHandler($this->judge(__FUNCTION__))->lLen($key);
    }

    /**
     * 往set集合添加成员
     *
     * @param string $key key
     * @param string $member 成员
     * @return mixed
     */
    public function sAdd($key, $member) {
        return $this->getHandler($this->judge(__FUNCTION__))->sAdd($key, $member);
    }

    /**
     * 获取集合所有成员
     *
     * @param string $key key
     * @return mixed
     */
    public function sMembers($key) {
        return $this->getHandler($this->judge(__FUNCTION__))->sMembers($key);
    }

    /**
     * 从集合中移除指定的成员
     *
     * @param string $key key
     * @param string $member 成员
     * @return mixed
     */
    public function sRem($key, $member) {
        return $this->getHandler($this->judge(__FUNCTION__))->sRem($key, $member);
    }

    /**
     * 往有序集合添加成员
     *
     * @param string $key key
     * @param int $score score
     * @param string $value value
     * @return mixed
     */
    public function zAdd($key, $score, $value) {
        return $this->getHandler($this->judge(__FUNCTION__))->zAdd($key, $score, $value);
    }

    /**
     * 获取有序集合中成员的score
     *
     * @param string $key
     * @param string $member
     * @return mixed
     */
    public function zScore($key, $member) {
        return $this->getHandler($this->judge(__FUNCTION__))->zScore($key, $member);
    }

    /**
     * 增加有序集合score值
     *
     * @param string $key key
     * @param int $increment 增长的数值
     * @param string $value value值
     * @return mixed
     */
    public function zIncrBy($key, $increment, $value) {
        return $this->getHandler($this->judge(__FUNCTION__))->zIncrBy($key, $increment, $value);
    }

    /**
     * 从有序集合获取指定范围内的成员（按score从小到大）
     *
     * @param string $key key
     * @param int $start 起始值
     * @param int $stop 截止值
     * @param bool $isWithScore 是否包含score值
     * @return mixed
     */
    public function zRange($key, $start, $stop, $isWithScore = false) {
        return $this->getHandler($this->judge(__FUNCTION__))->zRange($key, $start, $stop, $isWithScore);
    }

    /**
     * 获取有序集合指定范围内的元素，根据score从高到低
     *
     * @param string $key
     * @param int $start
     * @param int $stop
     * @param bool $isWithScore
     * @return mixed
     */
    public function zRevRange($key, $start, $stop, $isWithScore = false) {
        return $this->getHandler($this->judge(__FUNCTION__))->zRevRange($key, $start, $stop, $isWithScore);
    }

    /**
     * 获取score在max和min之间的所有元素
     *
     * @param string $key
     * @param int $max 最大值
     * @param int $min 最小值
     * @param bool $isWithScore
     * @return mixed
     */
    public function zRevRangeByScore($key, $max, $min, $isWithScore = false) {
        return $this->getHandler($this->judge(__FUNCTION__))->zRevRangeByScore($key, $max, $min, $isWithScore);

    }

    /**
     * 获取member的排名，排名根据score从高到低，排名从0开始
     *
     * @param string $key
     * @param string $member
     * @return mixed
     */
    public function zRevRank($key, $member) {
        return $this->getHandler($this->judge(__FUNCTION__))->zRevRank($key, $member);
    }

    /**
     * 从有序集合移除指定的成员
     *
     * @param string $key key
     * @param string $member 成员
     * @return mixed
     */
    public function zRem($key, $member) {
        return $this->getHandler($this->judge(__FUNCTION__))->zRem($key, $member);
    }

    /**
     * 从有序集合中移除指定排名范围内的成员
     *
     * @param string $key key
     * @param int $start 起始排名 （包含） 从0开始
     * @param int $stop 截止排名 （包含） 从0开始
     * @return mixed
     */
    public function zRemRangeByRank($key, $start, $stop) {
        return $this->getHandler($this->judge(__FUNCTION__))->zRemRangeByRank($key, $start, $stop);
    }

    /**
     * 从有序集合中移除指定score范围内的成员
     *
     * @param string $key key
     * @param int $min 起始score （包含）
     * @param int $max 截止score （包含）
     * @return mixed
     */
    public function zRemRangeByScore($key, $min, $max) {
        return $this->getHandler($this->judge(__FUNCTION__))->zRemRangeByScore($key, $min, $max);
    }

    /**
     * 当访问不存在或者没权限访问的方法时会访问此方法
     *
     * @param $method
     * @param $parameters
     * @return mixed
     */
    public function __call($method, $parameters) {
        return $this->getHandler($this->judge($method))->{$method}(...$parameters);
    }

}



