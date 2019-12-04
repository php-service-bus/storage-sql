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
use Amp\Postgres\Transaction as AmpTransaction;
use Amp\Promise;
use Psr\Log\LoggerInterface;
use ServiceBus\Storage\Common\Transaction;

/**
 * Async PostgreSQL transaction adapter.
 *
 * @internal
 */
final class AmpPostgreSQLTransaction implements Transaction
{
    /** @var AmpTransaction */
    private $transaction;

    /** @var LoggerInterface */
    private $logger;

    public function __construct(AmpTransaction $transaction, LoggerInterface $logger)
    {
        $this->transaction = $transaction;
        $this->logger      = $logger;
    }

    public function __destruct()
    {
        if ($this->transaction->isAlive() === true)
        {
            $this->transaction->close();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $queryString, array $parameters = []): Promise
    {
        $transaction = $this->transaction;
        $logger      = $this->logger;

        /** @psalm-suppress InvalidArgument */
        return call(
        /** @psalm-return AmpPostgreSQLResultSet */
            static function (string $queryString, array $parameters = []) use ($transaction, $logger): \Generator
            {
                try
                {
                    $logger->debug($queryString, $parameters);

                    /** @psalm-suppress TooManyTemplateParams */
                    return new AmpPostgreSQLResultSet(
                        yield $transaction->execute($queryString, $parameters)
                    );
                }
                // @codeCoverageIgnoreStart
                catch (\Throwable $throwable)
                {
                    throw adaptAmpThrowable($throwable);
                }
                // @codeCoverageIgnoreEnd
            },
            $queryString,
            $parameters
        );
    }

    /**
     * {@inheritdoc}
     */
    public function commit(): Promise
    {
        $transaction = $this->transaction;
        $logger      = $this->logger;

        return call(
            static function () use ($transaction, $logger): \Generator
            {
                try
                {
                    $logger->debug('COMMIT');

                    /** @psalm-suppress TooManyTemplateParams */
                    yield $transaction->commit();

                    $transaction->close();
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
    public function rollback(): Promise
    {
        $transaction = $this->transaction;
        $logger      = $this->logger;

        return call(
            static function () use ($transaction, $logger): \Generator
            {
                try
                {
                    $logger->debug('ROLLBACK');

                    /** @psalm-suppress TooManyTemplateParams */
                    yield $transaction->rollback();
                }
                // @codeCoverageIgnoreStart
                catch (\Throwable $throwable)
                {
                    /** We will not throw an exception */
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
        if (\is_resource($payload) === true)
        {
            $payload = \stream_get_contents($payload, -1, 0);
        }

        return \pg_unescape_bytea((string) $payload);
    }
}
