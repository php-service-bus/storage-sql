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
    /**
     * Original transaction object.
     *
     * @var AmpTransaction
     */
    private $transaction;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @param AmpTransaction  $transaction
     * @param LoggerInterface $logger
     */
    public function __construct(AmpTransaction $transaction, LoggerInterface $logger)
    {
        $this->transaction = $transaction;
        $this->logger      = $logger;
    }

    public function __destruct()
    {
        if (true === $this->transaction->isAlive())
        {
            $this->transaction->close();
        }
    }

    /**
     * @psalm-suppress MixedTypeCoercion
     *
     * {@inheritdoc}
     */
    public function execute(string $queryString, array $parameters = []): Promise
    {
        /** @psalm-suppress InvalidArgument */
        return call(
        /** @psalm-return AmpPostgreSQLResultSet */
            function(string $queryString, array $parameters = []): \Generator
            {
                try
                {
                    $this->logger->debug($queryString, $parameters);

                    /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                    return new AmpPostgreSQLResultSet(
                        yield $this->transaction->execute($queryString, $parameters)
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
        /**
         * @psalm-suppress InvalidArgument
         * @psalm-suppress MixedTypeCoercion
         */
        return call(
            function(): \Generator
            {
                try
                {
                    $this->logger->debug('COMMIT');

                    /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                    yield $this->transaction->commit();

                    $this->transaction->close();
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
        /**
         * @psalm-suppress InvalidArgument
         * @psalm-suppress MixedTypeCoercion
         */
        return call(
            function(): \Generator
            {
                try
                {
                    $this->logger->debug('ROLLBACK');

                    /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                    yield $this->transaction->rollback();
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
        if (true === \is_resource($payload))
        {
            $payload = \stream_get_contents($payload, -1, 0);
        }

        return \pg_unescape_bytea((string) $payload);
    }
}
