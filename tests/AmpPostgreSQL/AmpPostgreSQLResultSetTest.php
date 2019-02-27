<?php

/**
 * SQL databases adapters implementation.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql\Tests\AmpPostgreSQL;

use function Amp\Promise\wait;
use function ServiceBus\Storage\Sql\AmpPosgreSQL\postgreSqlAdapterFactory;
use function ServiceBus\Storage\Sql\fetchAll;
use function ServiceBus\Storage\Sql\fetchOne;
use PHPUnit\Framework\Constraint\IsType;
use PHPUnit\Framework\TestCase;
use ServiceBus\Storage\Sql\AmpPosgreSQL\AmpPostgreSQLAdapter;

/**
 *
 */
final class AmpPostgreSQLResultSetTest extends TestCase
{
    /**
     * @var AmpPostgreSQLAdapter
     */
    private $adapter;

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->adapter = postgreSqlAdapterFactory((string) \getenv('TEST_POSTGRES_DSN'));

        wait(
            $this->adapter->execute(
                'CREATE TABLE IF NOT EXISTS test_result_set (id uuid PRIMARY KEY, value VARCHAR)'
            )
        );
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    protected function tearDown(): void
    {
        parent::tearDown();

        wait(
            $this->adapter->execute('DROP TABLE test_result_set')
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function fetchOne(): void
    {
        $uuid1 = '3b5f80dd-0d14-4f8e-9684-0320dc35d3fd';
        $uuid2 = 'ad1278ad-031a-45e0-aa04-2a03e143d438';

        $promise = $this->adapter->execute(
            'INSERT INTO test_result_set (id, value) VALUES (?,?), (?,?)',
            [
                $uuid1, 'value1',
                $uuid2, 'value2',
            ]
        );

        wait($promise);

        $result = wait(
            fetchOne(
                wait($this->adapter->execute(\sprintf('SELECT * FROM test_result_set WHERE id = \'%s\'', $uuid2)))
            )
        );

        static::assertNotEmpty($result);
        static:: assertSame(['id' => $uuid2, 'value' => 'value2'], $result);

        $result = wait(
            fetchOne(
                wait(
                    $this->adapter->execute('SELECT * FROM test_result_set WHERE id = \'b4141f6e-a461-11e8-98d0-529269fb1459\'')
                )
            )
        );

        static::assertNull($result);
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function fetchAll(): void
    {
        $promise = $this->adapter->execute(
            'INSERT INTO test_result_set (id, value) VALUES (?,?), (?,?)',
            [
                'b922bda9-d2e5-4b41-b30d-e3b9a3717753', 'value1',
                '3fdbbc08-c6bd-4fd9-b343-1c069c0d3044', 'value2',
            ]
        );

        wait($promise);

        $result = wait(
            fetchAll(
                wait($this->adapter->execute('SELECT * FROM test_result_set'))
            )
        );

        static::assertNotEmpty($result);
        static::assertCount(2, $result);
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function fetchAllWithEmptySet(): void
    {
        $result = wait(
            fetchAll(
                wait($this->adapter->execute('SELECT * FROM test_result_set'))
            )
        );

        static::assertThat($result, new IsType('array'));
        static::assertEmpty($result);
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function multipleGetCurrentRow(): void
    {
        $promise = $this->adapter->execute(
            'INSERT INTO test_result_set (id, value) VALUES (?,?), (?,?)',
            [
                '457e634c-6fef-4144-a5e4-76def3f51c10', 'value1',
                'f4edd226-6fbf-499d-b6c4-b419560a7291', 'value2',
            ]
        );

        wait($promise);

        /** @var \ServiceBus\Storage\Common\ResultSet $result */
        $result = wait($this->adapter->execute('SELECT * FROM test_result_set'));

        while (wait($result->advance()))
        {
            $row     = $result->getCurrent();
            $rowCopy = $result->getCurrent();

            static::assertSame($row, $rowCopy);
        }
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function executeCommand(): void
    {
        /** @var \ServiceBus\Storage\Common\ResultSet $result */
        $result = wait($this->adapter->execute('DELETE FROM test_result_set'));

        while (wait($result->advance()))
        {
            static::fail('Non empty cycle');
        }

        static::assertNull(wait($result->lastInsertId()));
    }
}
