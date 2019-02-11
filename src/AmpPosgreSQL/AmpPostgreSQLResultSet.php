<?php

/**
 * SQL databases adapters implementation
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql\AmpPosgreSQL;

use function Amp\call;
use Amp\Postgres\PgSqlCommandResult;
use Amp\Postgres\PqCommandResult;
use Amp\Promise;
use Amp\Success;
use Amp\Sql\ResultSet as AmpResultSet;
use Amp\Postgres\PooledResultSet;
use ServiceBus\Storage\Common\Exceptions\ResultSetIterationFailed;
use ServiceBus\Storage\Common\ResultSet;

/**
 *
 */
class AmpPostgreSQLResultSet implements ResultSet
{
    /**
     * @var AmpResultSet|PooledResultSet
     */
    private $originalResultSet;

    /**
     * @var bool
     */
    private $advanceCalled = false;

    /**
     * @noinspection   PhpDocSignatureInspection
     * @psalm-suppress TypeCoercion Assume a different data type
     *
     * @param AmpResultSet|PooledResultSet $originalResultSet
     */
    public function __construct(object $originalResultSet)
    {
        $this->originalResultSet = $originalResultSet;
    }

    /**
     * @psalm-suppress MixedTypeCoercion
     *
     * @inheritdoc
     */
    public function advance(): Promise
    {
        $this->advanceCalled = true;

        try
        {
            if($this->originalResultSet instanceof AmpResultSet)
            {
                /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                return $this->originalResultSet->advance();
            }

            return new Success(false);
        }
            // @codeCoverageIgnoreStart
        catch(\Throwable $throwable)
        {
            throw new ResultSetIterationFailed($throwable->getMessage(), (int) $throwable->getCode(), $throwable);
        }
        // @codeCoverageIgnoreEnd
    }

    /**
     * @inheritdoc
     */
    public function getCurrent(): ?array
    {
        try
        {
            /** @var array<string, string|int|null|float|resource>|null $data */
            $data = $this->originalResultSet->getCurrent();

            return $data;
        }
            // @codeCoverageIgnoreStart
        catch(\Throwable $throwable)
        {
            throw new ResultSetIterationFailed($throwable->getMessage(), (int) $throwable->getCode(), $throwable);
        }
        // @codeCoverageIgnoreEnd
    }

    /**
     * @inheritdoc
     */
    public function lastInsertId(?string $sequence = null): Promise
    {
        /** @psalm-suppress InvalidArgument */
        return call(
            function(): \Generator
            {
                try
                {
                    if($this->originalResultSet instanceof PooledResultSet)
                    {
                        if(false === $this->advanceCalled)
                        {
                            yield $this->originalResultSet->advance();

                            $this->advanceCalled = true;
                        }

                        /** @var array<string, mixed> $result */
                        $result = $this->originalResultSet->getCurrent();

                        if(0 !== \count($result))
                        {
                            /** @var bool|int|string $value */
                            $value = \reset($result);

                            return false !== $value ? (string) $value : null;
                        }
                    }

                    return null;
                }
                    // @codeCoverageIgnoreStart
                catch(\Throwable $throwable)
                {
                    throw new ResultSetIterationFailed($throwable->getMessage(), (int) $throwable->getCode(), $throwable);
                }
                // @codeCoverageIgnoreEnd
            }
        );
    }

    /**
     * @inheritdoc
     */
    public function affectedRows(): int
    {
        try
        {
            if(
                $this->originalResultSet instanceof PgSqlCommandResult ||
                $this->originalResultSet instanceof PqCommandResult
            )
            {
                return $this->originalResultSet->getAffectedRowCount();
            }

            return 0;
        }
            // @codeCoverageIgnoreStart
        catch(\Throwable $throwable)
        {
            throw new ResultSetIterationFailed($throwable->getMessage(), (int) $throwable->getCode(), $throwable);
        }
        // @codeCoverageIgnoreEnd
    }
}
