<?php

/**
 * SQL databases adapters implementation.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql\DoctrineDBAL;

use function Amp\call;
use Amp\Failure;
use Amp\Promise;
use Amp\Success;
use Doctrine\DBAL\Connection;
use Psr\Log\LoggerInterface;
use ServiceBus\Storage\Common\Transaction;

/**
 * @internal
 */
final class DoctrineDBALTransaction implements Transaction
{
    private Connection $connection;

    private LoggerInterface $logger;

    public function __construct(Connection $connection, LoggerInterface $logger)
    {
        $this->connection = $connection;
        $this->logger     = $logger;
    }

    /**
     * @psalm-suppress MixedTypeCoercion
     *
     * {@inheritdoc}
     */
    public function execute(string $queryString, array $parameters = []): Promise
    {
        $this->logger->debug($queryString, $parameters);

        try
        {
            $statement = $this->connection->prepare($queryString);
            $isSuccess = $statement->execute($parameters);

            if (false === $isSuccess)
            {
                // @codeCoverageIgnoreStart
                /** @var string $message Driver-specific error message */
                $message = $this->connection->errorInfo()[2];

                throw new \RuntimeException($message);
                // @codeCoverageIgnoreEnd
            }

            return new Success(new DoctrineDBALResultSet($this->connection, $statement));
        }
        // @codeCoverageIgnoreStart
        catch (\Throwable $throwable)
        {
            return new Failure(adaptDbalThrowable($throwable));
        }
        // @codeCoverageIgnoreEnd
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
            function(): void
            {
                try
                {
                    $this->logger->debug('COMMIT');

                    $this->connection->commit();
                }
                // @codeCoverageIgnoreStart
                catch (\Throwable $throwable)
                {
                    throw adaptDbalThrowable($throwable);
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
            function(): void
            {
                try
                {
                    $this->logger->debug('ROLLBACK');

                    $this->connection->rollBack();
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
        /** @var resource|string $payload */
        if (true === \is_resource($payload))
        {
            $result = \stream_get_contents($payload, -1, 0);

            if (false !== $result)
            {
                return $result;
            }
        }

        return (string) $payload;
    }
}
