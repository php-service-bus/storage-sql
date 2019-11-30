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

use Amp\Promise;
use Amp\Success;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\Statement;
use ServiceBus\Storage\Common\ResultSet;

/**
 *
 */
final class DoctrineDBALResultSet implements ResultSet
{
    /**
     * Last row emitted.
     */
    private ?array

 $currentRow;

    /**
     * Pdo fetch result.
     */
    private array

 $fetchResult;

    /**
     * Results count.
     */
    private int $resultsCount;

    /**
     * Current iterator position.
     */
    private int $currentPosition = 0;

    /**
     * Connection instance.
     */
    private Connection $connection;

    /**
     * Number of rows affected by the last DELETE, INSERT, or UPDATE statement.
     */
    private int $affectedRows;

    public function __construct(Connection $connection, Statement $wrappedStmt)
    {
        $rows = $wrappedStmt->fetchAll();

        $this->connection   = $connection;
        $this->fetchResult  = $rows;
        $this->affectedRows = $wrappedStmt->rowCount();
        $this->resultsCount = \count($this->fetchResult);
    }

    /**
     * @psalm-suppress MixedTypeCoercion
     *
     * {@inheritdoc}
     */
    public function advance(): Promise
    {
        $this->currentRow = null;

        if (++$this->currentPosition > $this->resultsCount)
        {
            return new Success(false);
        }

        return new Success(true);
    }

    /**
     * {@inheritdoc}
     */
    public function getCurrent(): ?array
    {
        if (null !== $this->currentRow)
        {
            /**
             * @psalm-var array<string, float|int|resource|string|null>|null $row
             *
             * @var array $row
             */
            $row = $this->currentRow;

            return $row;
        }

        /**
         * @psalm-var array<string, float|int|resource|string|null>|null $data
         *
         * @var array $row
         */
        $data = $this->fetchResult[$this->currentPosition - 1] ?? null;

        if (true === \is_array($data) && 0 === \count($data))
        {
            $data = null;
        }

        return $this->currentRow = $data;
    }

    /**
     * @psalm-suppress MixedTypeCoercion
     *
     * {@inheritdoc}
     */
    public function lastInsertId(?string $sequence = null): Promise
    {
        return new Success($this->connection->lastInsertId($sequence));
    }

    /**
     * {@inheritdoc}
     */
    public function affectedRows(): int
    {
        return $this->affectedRows;
    }
}
