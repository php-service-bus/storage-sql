<?php

/**
 * SQL databases adapters implementation
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql\Tests;

use PHPUnit\Framework\TestCase;
use function ServiceBus\Storage\Sql\cast;
use function ServiceBus\Storage\Sql\deleteQuery;
use function ServiceBus\Storage\Sql\equalsCriteria;
use function ServiceBus\Storage\Sql\insertQuery;
use function ServiceBus\Storage\Sql\notEqualsCriteria;
use function ServiceBus\Storage\Sql\selectQuery;
use function ServiceBus\Storage\Sql\toSnakeCase;
use function ServiceBus\Storage\Sql\updateQuery;

/**
 *
 */
final class QueryBuilderFunctionsTest extends TestCase
{
    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function selectQuery(): void
    {
        $query = selectQuery('test', 'id', 'value')
            ->where(equalsCriteria('id', '100500'))
            ->compile();

        static::assertSame(
            'SELECT "id", "value" FROM "test" WHERE "id" = ?', $query->sql()
        );

        static::assertEquals(['100500'], $query->params());
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function updateQuery(): void
    {
        $query = updateQuery('test', ['name' => 'newName', 'email' => 'newEmail'])
            ->where(equalsCriteria('id', '100500'))
            ->compile();

        static::assertSame(
            'UPDATE "test" SET "name" = ?, "email" = ? WHERE "id" = ?', $query->sql()
        );

        static::assertEquals(['newName', 'newEmail', '100500'], $query->params());
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function deleteQuery(): void
    {
        $query = deleteQuery('test')->compile();

        static::assertSame('DELETE FROM "test"', $query->sql());
        static::assertEmpty($query->params());
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function insertQueryFromObject(): void
    {
        $object = new class('qwerty', 'root')
        {
            private $first;
            private $second;

            /**
             * @param $first
             * @param $second
             */
            public function __construct($first, $second)
            {
                /** @noinspection UnusedConstructorDependenciesInspection */
                $this->first = $first;
                /** @noinspection UnusedConstructorDependenciesInspection */
                $this->second = $second;
            }
        };

        $query = insertQuery('test', $object)->compile();

        static::assertSame(
            'INSERT INTO "test" ("first", "second") VALUES (?, ?)',
            $query->sql()
        );

        static::assertEquals(['qwerty', 'root'], $query->params());
    }


    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function insertQueryFromArray(): void
    {
        $query = insertQuery('test', ['first' => 'qwerty', 'second' => 'root'])->compile();

        static::assertSame(
            'INSERT INTO "test" ("first", "second") VALUES (?, ?)',
            $query->sql()
        );

        static::assertEquals(['qwerty', 'root'], $query->params());
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function toSnakeCase(): void
    {
        static::assertSame(
            'some_snake_case', toSnakeCase('someSnakeCase')
        );
    }


    /**
     * @test
     * @expectedException \LogicException
     * @expectedExceptionMessage The "key" property must contain a scalar value. "array" given
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function castNonScalarType(): void
    {
        /** @noinspection PhpParamsInspection */
        cast('key', []);
    }

    /**
     * @test
     * @expectedException \LogicException
     * @expectedExceptionMessage "Closure" must implements "__toString" method
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function castObjectWithoutToString(): void
    {
        cast(
            'key',
            function()
            {

            }
        );
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function castObjectWithToString(): void
    {
        $object = new class()
        {
            public function __toString()
            {
                return 'qwerty';
            }
        };

        static::assertSame('qwerty', cast('key', $object));
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function objectNotEqualsCriteria(): void
    {
        $object = new class()
        {
            /** @var string */
            private $id;

            public function __construct()
            {
                $this->id = 'uuid';
            }

            public function __toString()
            {
                return $this->id;
            }
        };

        $query = selectQuery('test')->where(notEqualsCriteria('id', $object))->compile();

        static::assertEquals('SELECT * FROM "test" WHERE "id" != ?', $query->sql());
        static::assertEquals([(string) $object], $query->params());
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function scalarNotEqualsCriteria(): void
    {
        $id = 'uuid';

        $query = selectQuery('test')->where(notEqualsCriteria('id', $id))->compile();

        static::assertEquals('SELECT * FROM "test" WHERE "id" != ?', $query->sql());
        static::assertEquals([$id], $query->params());
    }
}
