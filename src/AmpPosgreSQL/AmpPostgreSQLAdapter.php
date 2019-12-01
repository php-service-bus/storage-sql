<?php

/**
 * SQL databases adapters implementation.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql\AmpPosgreSQL;

use function Amp\call;
use function Amp\Postgres\pool;
use Amp\Coroutine;
use Amp\Postgres\ConnectionConfig;
use Amp\Postgres\Pool;
use Amp\Promise;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ServiceBus\Storage\Common\DatabaseAdapter;
use ServiceBus\Storage\Common\Exceptions\InvalidConfigurationOptions;
use ServiceBus\Storage\Common\StorageConfiguration;

/**
 * @see https://github.com/amphp/postgres
 */
final class AmpPostgreSQLAdapter implements DatabaseAdapter
{
    private StorageConfiguration $configuration;

    private ?Pool $pool;

    private LoggerInterface $logger;

    /**
     * @throws \ServiceBus\Storage\Common\Exceptions\InvalidConfigurationOptions
     */
    public function __construct(StorageConfiguration $configuration, ?LoggerInterface $logger = null)
    {
        // @codeCoverageIgnoreStart
        if (false === \extension_loaded('pgsql'))
        {
            throw new InvalidConfigurationOptions('ext-pgsql must be installed');
        }
        // @codeCoverageIgnoreEnd

        $this->configuration = $configuration;
        $this->logger        = $logger ?? new NullLogger();
    }

    public function __destruct()
    {
        /** @psalm-suppress RedundantConditionGivenDocblockType Null in case of error */
        if (isset($this->pool))
        {
            $this->pool->close();
        }
    }

    /**
     * @psalm-suppress MixedTypeCoercion
     *
     * {@inheritdoc}
     */
    public function execute(string $queryString, array $parameters = []): Promise
    {
        $pool   = $this->pool();
        $logger = $this->logger;

        /** @psalm-suppress InvalidArgument */
        return call(
        /** @psalm-return AmpPostgreSQLResultSet */
            static function (string $queryString, array $parameters = []) use ($pool, $logger): \Generator
            {
                try
                {
                    $logger->debug($queryString, $parameters);

                    return new AmpPostgreSQLResultSet(
                        yield $pool->execute($queryString, $parameters)
                    );
                }
                catch (\Throwable $throwable)
                {
                    throw adaptAmpThrowable($throwable);
                }
            },
            $queryString,
            $parameters
        );
    }

    /**
     * @psalm-suppress MixedTypeCoercion
     *
     * {@inheritdoc}
     */
    public function transactional(callable $function): Promise
    {
        $pool   = $this->pool();
        $logger = $this->logger;

        return call(
            static function () use ($pool, $logger, $function): \Generator
            {
                /** @var \Amp\Postgres\Transaction $originalTransaction  */
                $originalTransaction = yield $pool->beginTransaction();

                $transaction = new AmpPostgreSQLTransaction($originalTransaction, $logger);

                $logger->debug('BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED');

                try
                {
                    /** @var \Generator $generator */
                    $generator = $function($transaction);

                    yield new Coroutine($generator);

                    yield $transaction->commit();
                }
                catch (\Throwable $throwable)
                {
                    yield $transaction->rollback();

                    throw $throwable;
                }
                finally
                {
                    unset($transaction);
                }
            }
        );
    }

    /**
     * @psalm-suppress MixedTypeCoercion
     *
     * {@inheritdoc}
     */
    public function transaction(): Promise
    {
        $pool   = $this->pool();
        $logger = $this->logger;

        return call(
            static function () use ($pool, $logger): \Generator
            {
                try
                {
                    $logger->debug('BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED');

                    /** @var \Amp\Postgres\Transaction $transaction */
                    $transaction = yield $pool->beginTransaction();

                    return new AmpPostgreSQLTransaction($transaction, $logger);
                }
                // @codeCoverageIgnoreStart
                catch (\Throwable $throwable)
                {
                    throw adaptAmpThrowable($throwable);
                }
                // @codeCoverageIgnoreEnd
            }
        );
    }

    /**
     * {@inheritdoc}
     */
    public function unescapeBinary($payload): string
    {
        if (true === \is_resource($payload))
        {
            $payload = \stream_get_contents($payload, -1, 0);
        }

        return \pg_unescape_bytea((string) $payload);
    }

    /**
     * Receive connection pool.
     */
    private function pool(): Pool
    {
        if (false === isset($this->pool))
        {
            $queryData = $this->configuration->queryParameters;

            $maxConnectionsCount = (int) ($queryData['max_connections'] ?? Pool::DEFAULT_MAX_CONNECTIONS);
            $idleTimeout         = (int) ($queryData['idle_timeout'] ?? Pool::DEFAULT_IDLE_TIMEOUT);

            $this->pool = pool(
                new ConnectionConfig(
                    (string) $this->configuration->host,
                    $this->configuration->port ?? ConnectionConfig::DEFAULT_PORT,
                    (string) $this->configuration->username,
                    (string) $this->configuration->password,
                    (string) $this->configuration->databaseName
                ),
                $maxConnectionsCount,
                $idleTimeout
            );
        }

        return $this->pool;
    }
}
