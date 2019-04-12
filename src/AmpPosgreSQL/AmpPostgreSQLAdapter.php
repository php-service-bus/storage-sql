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
 *
 */
final class AmpPostgreSQLAdapter implements DatabaseAdapter
{
    /**
     * Connection parameters.
     *
     * @var StorageConfiguration
     */
    private $configuration;

    /**
     * Connections pool.
     *
     * @var Pool|null
     */
    private $pool;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @param StorageConfiguration $configuration
     * @param LoggerInterface|null $logger
     *
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
        if (null !== $this->pool)
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
            static function(string $queryString, array $parameters = []) use ($pool, $logger): \Generator
            {
                try
                {
                    $logger->debug($queryString, $parameters);

                    /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
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
            static function() use ($pool, $logger, $function): \Generator
            {
                /**
                 * @psalm-suppress TooManyTemplateParams
                 *
                 * @var \Amp\Postgres\Transaction $originalTransaction
                 */
                $originalTransaction = yield $pool->beginTransaction();

                /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                $transaction = new AmpPostgreSQLTransaction($originalTransaction, $logger);

                $logger->debug('BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED');

                try
                {
                    /** @var \Generator $generator */
                    $generator = $function($transaction);

                    yield new Coroutine($generator);

                    /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                    yield $transaction->commit();
                }
                catch (\Throwable $throwable)
                {
                    /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
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

        /** @psalm-suppress InvalidArgument */
        return call(
            static function() use ($pool, $logger): \Generator
            {
                try
                {
                    $logger->debug('BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED');

                    /**
                     * @psalm-suppress TooManyTemplateParams Wrong Promise template
                     *
                     * @var \Amp\Postgres\Transaction $transaction
                     */
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
     *
     * @return Pool
     */
    private function pool(): Pool
    {
        if (null === $this->pool)
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
