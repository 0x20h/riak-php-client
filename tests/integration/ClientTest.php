<?php
namespace Basho\Riak;

class ClientTest extends \PHPUnit_Framework_TestCase {

    private $client;

    public function setUp()
    {
        $this->client = new Riak($_ENV['RIAK_HOST'], $_ENV['RIAK_PORT']);
    }

    public function tearDown()
    {
    }

    public function testIsAlive()
    {
        $this->assertTrue($this->client->isAlive());
    }

    public function testStoreAndGet()
    {
        $bucket = $this->client->bucket('bucket');

        $rand = rand();
        $obj = $bucket->newObject('foo', $rand);
        $obj->store();

        $obj = $bucket->get('foo');

        $this->assertTrue($obj->exists());
        $this->assertEquals($obj->getBucket()->getName(), 'bucket');
        $this->assertEquals($obj->getKey(), 'foo');
        $this->assertEquals($obj->getData(), $rand);
    }

    public function testStoreAndGetWithoutKey()
    {
        $bucket = $this->client->bucket('bucket');

        $rand = rand();
        $obj = $bucket->newObject(null, $rand);
        $obj->store();

        $key = $obj->key;

        $obj = $bucket->get($key);
        $this->assertTrue($obj->exists());
        $this->assertEquals($obj->getBucket()->getName(), 'bucket');
        $this->assertEquals($obj->getKey(), $key);
        $this->assertEquals($obj->getData(), $rand);
    }
}
