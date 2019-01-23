<?php

/**
 * PHP Service Bus SQL storage implementation
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
    /**
     * DBAL connection
     *
     * @var Connection
     */
    private $connection;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @param Connection      $connection
     * @param LoggerInterface $logger
     */
    public function __construct(Connection $connection, LoggerInterface $logger)
    {
        $this->connection = $connection;
        $this->logger     = $logger;
    }

    /**
     * @inheritdoc
     */
    public function execute(string $queryString, array $parameters = []): Promise
    {
        $this->logger->debug($queryString, $parameters);

        try
        {
            $statement = $this->connection->prepare($queryString);
            $isSuccess = $statement->execute($parameters);

            if(false === $isSuccess)
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
        catch(\Throwable $throwable)
        {
            return new Failure(adaptDbalThrowable($throwable));
        }
        // @codeCoverageIgnoreEnd
    }

    /**
     * @inheritdoc
     */
    public function commit(): Promise
    {
        $connection = $this->connection;
        $logger     = $this->logger;

        /** InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            static function() use ($connection, $logger): void
            {
                try
                {
                    $logger->debug('COMMIT');

                    $connection->commit();
                }
                    // @codeCoverageIgnoreStart
                catch(\Throwable $throwable)
                {
                    throw adaptDbalThrowable($throwable);
                }
                // @codeCoverageIgnoreEnd
            }
        );
    }

    /**
     * @inheritdoc
     */
    public function rollback(): Promise
    {
        $connection = $this->connection;
        $logger     = $this->logger;

        /** InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            static function() use ($connection, $logger): void
            {
                try
                {
                    $logger->debug('ROLLBACK');

                    $connection->rollBack();
                }
                    // @codeCoverageIgnoreStart
                catch(\Throwable $throwable)
                {
                    /** We will not throw an exception */
                }
                // @codeCoverageIgnoreEnd
            }
        );
    }

    /**
     * @inheritDoc
     */
    public function unescapeBinary($payload): string
    {
        /** @var string|resource $payload */

        if(true === \is_resource($payload))
        {
            return \stream_get_contents($payload, -1, 0);
        }

        return $payload;
    }
}
