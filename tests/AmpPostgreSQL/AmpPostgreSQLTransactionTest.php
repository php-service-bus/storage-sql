<?php

/**
 * PHP Service Bus (publish-subscribe pattern implementation) storage sql implementation
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql\Tests\AmpPostgreSQL;

use function Amp\Promise\wait;
use ServiceBus\Storage\Common\DatabaseAdapter;
use ServiceBus\Storage\Sql\AmpPosgreSQL\AmpPostgreSQLAdapter;
use function ServiceBus\Storage\Sql\AmpPosgreSQL\postgreSqlAdapterFactory;
use ServiceBus\Storage\Sql\Tests\BaseTransactionTest;

/**
 *
 */
final class AmpPostgreSQLTransactionTest extends BaseTransactionTest
{
    /**
     * @var AmpPostgreSQLAdapter
     */
    private static $adapter;

    /**
     * @inheritdoc
     *
     * @throws \Throwable
     */
    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        $adapter = static::getAdapter();

        wait(
            $adapter->execute(
                'CREATE TABLE IF NOT EXISTS test_result_set (id uuid PRIMARY KEY, value bytea)'
            )
        );
    }

    /**
     * @inheritdoc
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
     *
     * @throws \Throwable
     */
    protected static function getAdapter(): DatabaseAdapter
    {
        if(null === self::$adapter)
        {
            self::$adapter = postgreSqlAdapterFactory((string) \getenv('TEST_POSTGRES_DSN'));
        }

        return self::$adapter;
    }
}
