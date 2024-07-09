<?php

declare(strict_types=1);

namespace App\Loader;

use Bookeen\ETLWorkflow\Context\ContextInterface;

class DynamicKeyRedisLoader extends RedisLoader
{
    protected array $redisKeyPrefixes = [];

    protected array $redisKeySuffixes = [];

    protected array $dynamicVarsName = [];

    public function setRedisKeySuffixes(array|string $redisKeySuffixes): void
    {
        $this->redisKeySuffixes = \is_array($redisKeySuffixes) ? $redisKeySuffixes : [$redisKeySuffixes];
    }

    public function setRedisKeyPrefixes(array|string $redisKeyPrefixes): void
    {
        $this->redisKeyPrefixes = \is_array($redisKeyPrefixes) ? $redisKeyPrefixes : [$redisKeyPrefixes];
    }

    public function setDynamicVarsName(array|string $dynamicVarsName): void
    {
        $this->dynamicVarsName = \is_array($dynamicVarsName) ? $dynamicVarsName : [$dynamicVarsName];
    }

    public function load($data, ContextInterface $context): void
    {
        if (empty($data)) {
            return;
        }

        $key = implode('.', array_filter([
            ...$this->redisKeyPrefixes,
            ...array_map(static fn (string $var) => $data[$var] ?? null, $this->dynamicVarsName),
            ...$this->redisKeySuffixes,
        ]));
        $value = json_encode($data, JSON_THROW_ON_ERROR);
        
        $this->redis->set($key, $value);
    }
}
