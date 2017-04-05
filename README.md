
# Redis

#### Installation
```
composer require comoyi/redis
```

#### Usage
```
<?php

use Comoyi\Redis\RedisClient;

$config = [
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

$redis = new RedisClient($config);
$key = 'just-for-test';
$redis->set($key, 'test data');
$data = $redis->get($key);
var_dump($data);
```