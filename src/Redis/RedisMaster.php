<?php

namespace Comoyi\Redis;

use Redis;

/**
 * redis master
 */
class RedisMaster {

    /**
     * 配置
     *
     * @var array
     */
    protected $configs = [];

    /**
     * Redis
     *
     * @var Redis
     */
    protected $handler = null;

    /**
     * 构造
     *
     * @param array $config
     */
    public function __construct($config) {
        $defaultConfig = [
            'host' => '',
            'port' => '',
            'password' => '',
        ];
        $this->setConfigs($defaultConfig);
        $this->setConfigs($config);

        $this->connect();

        if('' !== $this->configs['password']) {
            $this->auth();
        }
    }

    /**
     * 设置配置
     *
     * @param array $configs
     */
    public function setConfigs($configs) {
        $this->configs = array_merge($this->configs, $configs);
    }

    /**
     * 连接
     */
    public function connect() {
        $this->handler = new Redis();
        $this->handler->connect($this->configs['host'], $this->configs['port']);
    }

    /**
     * 验证
     */
    public function auth() {
        $this->handler->auth($this->configs['password']);
    }

    /**
     * 获取连接
     */
    public function getHandler() {
        return $this->handler;
    }

}
