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
        $transaction = $this->transaction;
        $logger = $this->logger;

        /** @psalm-suppress InvalidArgument */
        return call(
        /** @psalm-return AmpPostgreSQLResultSet */
           static function(string $queryString, array $parameters = []) use ($transaction, $logger): \Generator
           {
               try
               {
                   $logger->debug($queryString, $parameters);

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
        $logger = $this->logger;

        /** @psalm-suppress MixedTypeCoercion */
        return call(
            static function() use ($transaction, $logger): \Generator
            {
                try
                {
                    $logger->debug('COMMIT');

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
        $logger = $this->logger;

        /** @psalm-suppress MixedTypeCoercion */
        return call(
            static function() use ($transaction, $logger): \Generator
            {
                try
                {
                    $logger->debug('ROLLBACK');

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
        if (true === \is_resource($payload))
        {
            $payload = \stream_get_contents($payload, -1, 0);
        }

        return \pg_unescape_bytea((string) $payload);
    }
}
