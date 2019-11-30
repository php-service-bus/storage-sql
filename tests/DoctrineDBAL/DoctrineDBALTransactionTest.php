<?php

/**
 * SQL databases adapters implementation.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql\Tests\DoctrineDBAL;

use function Amp\Promise\wait;
use function ServiceBus\Storage\Sql\DoctrineDBAL\inMemoryAdapter;
use ServiceBus\Storage\Common\DatabaseAdapter;
use ServiceBus\Storage\Sql\DoctrineDBAL\DoctrineDBALAdapter;
use ServiceBus\Storage\Sql\Tests\BaseTransactionTest;

/**
 *
 */
final class DoctrineDBALTransactionTest extends BaseTransactionTest
{
    private static DoctrineDBALAdapter $adapter;

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        $adapter = static::getAdapter();

        wait(
            $adapter->execute(
                'CREATE TABLE IF NOT EXISTS test_result_set (id uuid PRIMARY KEY, value binary)'
            )
        );
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    public static function tearDownAfterClass(): void
    {
        parent::tearDownAfterClass();

        $adapter = static::getAdapter();

        wait(
            $adapter->execute('DROP TABLE test_result_set')
        );
    }

    /**
     * @return DatabaseAdapter
     */
    protected static function getAdapter(): DatabaseAdapter
    {
        if (isset(self::$adapter) === false)
        {
            self::$adapter = inMemoryAdapter();
        }

        return self::$adapter;
    }
}
