<?php
/**
 * Riak PHP Client
 *
 * NOTICE OF LICENSE
 *
 * This source file is subject to the Apache License, Version 2.0 that is
 * bundled with this package in the file LICENSE.
 * It is also available through the world-wide-web at this URL:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * If you did not receive a copy of the license and are unable to
 * obtain it through the world-wide-web, please send an email
 * to <eng@basho.com> so we can send you a copy immediately.
 *
 * @category   Riak
 * @package    TestSuite
 * @copyright  Copyright (c) 2013 Basho Technologies, Inc. and contributors.
 */
namespace Basho\Riak;

/**
 * TestSuite
 *
 * @category   Riak
 * @package    TestSuite
 * @author     Riak team (https://github.com/basho/riak-php-client/contributors)
 */
class ClientTest extends \PHPUnit_Framework_TestCase {

    private $client;

    public function setUp()
    {
        $this->client = new Riak($_ENV['RIAK_HOST'], $_ENV['RIAK_PORT']);
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

    public function testBinaryStoreAndGet()
    {
        $bucket = $this->client->bucket('bucket');

        # Store as binary, retrieve as binary, then compare...
        $rand = rand();
        $obj = $bucket->newBinary('foo1', $rand);
        $obj->store();
        $obj = $bucket->getBinary('foo1');
        $this->_assert($obj->exists());
        $this->_assert($obj->getData() == $rand);

        # Store as JSON, retrieve as binary, JSON-decode, then compare...
        $data = array(rand(), rand(), rand());
        $obj = $bucket->newObject('foo2', $data);
        $obj->store();
        $obj = $bucket->getBinary('foo2');
        $this->_assert($data == json_decode($obj->getData()));
    }

    public function testMissingObject()
    {
        $bucket = $this->client->bucket('bucket');
        $obj = $bucket->get("missing");
        $this->_assert(!$obj->exists());
        $this->_assert($obj->getData() == null);
    }

    public function testDelete()
    {
        $bucket = $this->client->bucket('bucket');

        $rand = rand();
        $obj = $bucket->newObject('foo', $rand);
        $obj->store();

        $obj = $bucket->get('foo');
        $this->_assert($obj->exists());

        $obj->delete();
        $obj->reload();
        $this->_assert(!$obj->exists());
    }

    public function testSetBucketProperties()
    {
        $bucket = $this->client->bucket('bucket');

        # Test setting allow mult...
        $bucket->setAllowMultiples(true);
        $this->_assert($bucket->getAllowMultiples());

        # Test setting nval...
        $bucket->setNVal(3);
        $this->_assert($bucket->getNVal() == 3);

        # Test setting multiple properties...
        $bucket->setProperties(array("allow_mult" => false, "n_val" => 2));
        $this->_assert(!$bucket->getAllowMultiples());
        $this->_assert($bucket->getNVal() == 2);
    }

    public function testSiblings()
    {
        # Set up the bucket, clear any existing object...
        $bucket = $this->client->bucket('multiBucket');
        $bucket->setAllowMultiples('true');
        $obj = $bucket->get('foo');
        $obj->delete();

        # Store the same object multiple times...
        for ($i = 0; $i < 5; $i++) {
            $client = new Riak($_ENV['RIAK_HOST'], $_ENV['RIAK_PORT']);
            $bucket = $this->client->bucket('multiBucket');
            $obj = $bucket->newObject('foo', rand());
            $obj->store();
        }

        # Make sure the object has 5 siblings...
        $this->_assert($obj->hasSiblings());
        $this->_assert($obj->getSiblingCount() == 5);

        # Test getSibling()/getSiblings()...
        $siblings = $obj->getSiblings();
        $obj3 = $obj->getSibling(3);
        $this->_assert($siblings[3]->getData() == $obj3->getData());

        # Resolve the conflict, and then do a get...
        $obj3 = $obj->getSibling(3);
        $obj3->store();

        $obj->reload();
        $this->_assert($obj->getData() == $obj3->getData());

        # Clean up for next tests...
        $obj->delete();
    }

    public function testJavascriptSourceMap()
    {
        # Create the object...
        $bucket = $this->client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();

        # Run the map...
        $result = $this->client->
            add("bucket", "foo")->
            map("function (v) { return [JSON.parse(v.values[0].data)]; }")->
            run();
        $this->_assert($result == array(2));
    }

    public function testJavascriptNamedMap()
    {
        # Create the object...
        $bucket = $this->client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();

        # Run the map...
        $result = $this->client->
            add("bucket", "foo")->
            map("Riak.mapValuesJson")->
            run();
        $this->_assert($result == array(2));
    }

    public function testJavascriptSourceMapReduce()
    {
        # Create the object...
        $bucket = $this->client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();
        $bucket->newObject("bar", 3)->store();
        $bucket->newObject("baz", 4)->store();

        # Run the map...
        $result = $this->client->
            add("bucket", "foo")->
            add("bucket", "bar")->
            add("bucket", "baz")->
            map("function (v) { return [1]; }")->
            reduce("Riak.reduceSum")->
            run();
        $this->_assert($result[0] == 3);
    }

    public function testJavascriptNamedMapReduce()
    {
        # Create the object...
        $bucket = $this->client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();
        $bucket->newObject("bar", 3)->store();
        $bucket->newObject("baz", 4)->store();

        # Run the map...
        $result = $this->client->
            add("bucket", "foo")->
            add("bucket", "bar")->
            add("bucket", "baz")->
            map("Riak.mapValuesJson")->
            reduce("Riak.reduceSum")->
            run();
        $this->_assert($result == array(9));
    }

    public function testJavascriptBucketMapReduce()
    {
        # Create the object...
        $bucket = $this->client->bucket("bucket_" . rand());
        $bucket->newObject("foo", 2)->store();
        $bucket->newObject("bar", 3)->store();
        $bucket->newObject("baz", 4)->store();

        # Run the map...
        $result = $this->client->
            add($bucket->name)->
            map("Riak.mapValuesJson")->
            reduce("Riak.reduceSum")->
            run();
        $this->_assert($result == array(9));
    }

    public function testJavascriptArgMapReduce()
    {
        # Create the object...
        $bucket = $this->client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();

        # Run the map...
        $result = $this->client->
            add("bucket", "foo", 5)->
            add("bucket", "foo", 10)->
            add("bucket", "foo", 15)->
            add("bucket", "foo", -15)->
            add("bucket", "foo", -5)->
            map("function(v, arg) { return [arg]; }")->
            reduce("Riak.reduceSum")->
            run();
        $this->_assert($result == array(10));
    }

    public function testErlangMapReduce()
    {
        # Create the object...
        $bucket = $this->client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();
        $bucket->newObject("bar", 2)->store();
        $bucket->newObject("baz", 4)->store();

        # Run the map...
        $result = $this->client->
            add("bucket", "foo")->
            add("bucket", "bar")->
            add("bucket", "baz")->
            map(array("riak_kv_mapreduce", "map_object_value"))->
            reduce(array("riak_kv_mapreduce", "reduce_set_union"))->
            run();
        $this->_assert(count($result) == 2);
    }

    public function testMapReduceFromObject()
    {
        # Create the object...
        $bucket = $this->client->bucket("bucket");
        $bucket->newObject("foo", 2)->store();

        $obj = $bucket->get("foo");
        $result = $obj->map("Riak.mapValuesJson")->run();
        $this->_assert($result = array(2));
    }

    public function testKeyFilter()
    {
        # Create the object...
        $bucket = $this->client->bucket("filter_bucket");
        $bucket->newObject("foo_one", array("foo" => "one"))->store();
        $bucket->newObject("foo_two", array("foo" => "two"))->store();
        $bucket->newObject("foo_three", array("foo" => "three"))->store();
        $bucket->newObject("foo_four", array("foo" => "four"))->store();
        $bucket->newObject("moo_five", array("foo" => "five"))->store();

        $mapred = $this->client
            ->add($bucket->name)
            ->key_filter(array('tokenize', '_', 1), array('eq', 'foo'));
        $results = $mapred->run();
        $this->_assert(count($results) == 4);
    }

    public function testKeyFilterOperator()
    {
        # Create the object...
        $bucket = $this->client->bucket("filter_bucket");
        $bucket->newObject("foo_one", array("foo" => "one"))->store();
        $bucket->newObject("foo_two", array("foo" => "two"))->store();
        $bucket->newObject("foo_three", array("foo" => "three"))->store();
        $bucket->newObject("foo_four", array("foo" => "four"))->store();
        $bucket->newObject("moo_five", array("foo" => "five"))->store();

        $mapred = $this->client
            ->add($bucket->name)
            ->key_filter(array('starts_with', 'foo'))
            ->key_filter_or(array('ends_with', 'five'));
        $results = $mapred->run();
        $this->_assert(count($results) == 5);
    }


    public function testStoreAndGetLinks()
    {
        # Create the object...
        $bucket = $this->client->bucket("bucket");
        $bucket->newObject("foo", 2)->
            addLink($bucket->newObject("foo1"))->
            addLink($bucket->newObject("foo2"), "tag")->
            addLink($bucket->newObject("foo3"), "tag2!@#$%^&*")->
            store();

        $obj = $bucket->get("foo");
        $links = $obj->getLinks();
        $this->_assert(count($links) == 3);
    }

    public function testLinkWalking()
    {
        # Create the object...
        $bucket = $this->client->bucket("bucket");
        $bucket->newObject("foo", 2)->
            addLink($bucket->newObject("foo1", "test1")->store())->
            addLink($bucket->newObject("foo2", "test2")->store(), "tag")->
            addLink($bucket->newObject("foo3", "test3")->store(), "tag2!@#$%^&*")->
            store();

        $obj = $bucket->get("foo");
        $results = $obj->link("bucket")->run();
        $this->_assert(count($results) == 3);

        $results = $obj->link("bucket", "tag")->run();
        $this->_assert(count($results) == 1);
    }

    public function testSearchIntegration()
    {
        # Create some objects to search across...
        $bucket = $this->client->bucket("searchbucket");

        $bucket->setProperty('search', true);

        $bucket->newObject("one", array("foo" => "one", "bar" => "red"))->store();
        $bucket->newObject("two", array("foo" => "two", "bar" => "green"))->store();
        $bucket->newObject("three", array("foo" => "three", "bar" => "blue"))->store();
        $bucket->newObject("four", array("foo" => "four", "bar" => "orange"))->store();
        $bucket->newObject("five", array("foo" => "five", "bar" => "yellow"))->store();

        # Run some operations...
        $results = $this->client->search("searchbucket", "foo:one OR foo:two")->run();
        if (count($results) == 0) {
            $this->markTestSkipped(
                'Not running tests "testSearchIntegration()". ' .
                'Please ensure that you have installed the Riak Search ' .
                'hook on bucket \"searchbucket\" by running ' .
                '"bin/search-cmd install searchbucket".'
            );
            
            return;
        }

        $this->_assert(count($results) == 2);

        $results = $this->client->search(
            "searchbucket",
            "(foo:one OR foo:two OR foo:three OR foo:four) AND (NOT bar:green)"
        )->run();
        $this->_assert(count($results) == 3);
    }

    public function testSecondaryIndexes()
    {
        $bucket = $this->client->bucket("indextest");

        # Immediate tests to see if 2i is even supported w/ the backend
        try {
            $bucket->indexSearch("foo", "bar_bin", "baz");
        } catch (Exception $e) {
            if (strpos($e->__toString(), "indexes_not_supported") !== false) {
                return true;
            } else {
                throw $e;
            }
        }

        # Okay, continue with the rest of the tests
        $bucket
            ->newObject("one", array("foo" => 1, "bar" => "red"))
            ->addIndex("number", "int", 1)
            ->addIndex("text", "bin", "apple")
            ->addAutoIndex("foo", "int")
            ->addAutoIndex("bar", "bin")
            ->store();
        $bucket
            ->newObject("two", array("foo" => 2, "bar" => "green"))
            ->addIndex("number", "int", 2)
            ->addIndex("text", "bin", "avocado")
            ->addAutoIndex("foo", "int")
            ->addAutoIndex("bar", "bin")
            ->store();
        $bucket
            ->newObject("three", array("foo" => 3, "bar" => "blue"))
            ->addIndex("number", "int", 3)
            ->addIndex("text", "bin", "blueberry")
            ->addAutoIndex("foo", "int")
            ->addAutoIndex("bar", "bin")
            ->store();
        $bucket
            ->newObject("four", array("foo" => 4, "bar" => "orange"))
            ->addIndex("number", "int", 4)
            ->addIndex("text", "bin", "citrus")
            ->addAutoIndex("foo", "int")
            ->addAutoIndex("bar", "bin")
            ->store();
        $bucket
            ->newObject("five", array("foo" => 5, "bar" => "yellow"))
            ->addIndex("number", "int", 5)
            ->addIndex("text", "bin", "banana")
            ->addAutoIndex("foo", "int")
            ->addAutoIndex("bar", "bin")
            ->store();

        $bucket
            ->newObject("six", array("foo" => 6, "bar" => "purple"))
            ->addIndex("number", "int", 6)
            ->addIndex("number", "int", 7)
            ->addIndex("number", "int", 8)
            ->setIndex("text", "bin", array("x", "y", "z"))
            ->store();

        # Exact matches
        $results = $bucket->indexSearch("number", "int", 5);
        $this->_assert(count($results) == 1);

        $results = $bucket->indexSearch("text", "bin", "apple");
        $this->_assert(count($results) == 1);

        # Range searches
        $results = $bucket->indexSearch("foo", "int", 1, 3);
        $this->_assert(count($results) == 3);

        $results = $bucket->indexSearch("bar", "bin", "blue", "orange");
        $this->_assert(count($results) == 3);

        # Test duplicate key de-duping
        $results = $bucket->indexSearch("number", "int", 6, 8, true);
        $this->_assert(count($results) == 1);

        $results = $bucket->indexSearch("text", "bin", "x", "z", true);
        $this->_assert(count($results) == 1);

        # Test auto indexes don't leave cruft indexes behind, and regular
        # indexes are preserved
        $object = $bucket->get("one");
        $object->setData(array("foo" => 9, "bar" => "plaid"));
        $object->store();

        # Auto index updates
        $results = $bucket->indexSearch("foo", "int", 9);
        $this->_assert(count($results) == 1);

        # Auto index leaves no cruft
        $results = $bucket->indexSearch("foo", "int", 1);
        $this->_assert(count($results) == 0);

        # Normal index is preserved
        $results = $bucket->indexSearch("number", "int", 1);
        $this->_assert(count($results) == 1);


        # Test proper collision handling on autoIndex and regular index on same field
        $bucket
            ->newObject("seven", array("foo" => 7))
            ->addAutoIndex("foo", "int")
            ->addIndex("foo", "int", 7)
            ->store();

        $results = $bucket->indexSearch("foo", "int", 7);
        $this->_assert(count($results) == 1);

        $object = $bucket->get("seven");
        $object->setData(array("foo" => 8));
        $object->store();

        $results = $bucket->indexSearch("foo", "int", 8);
        $this->_assert(count($results) == 1);

        $results = $bucket->indexSearch("foo", "int", 7);
        $this->_assert(count($results) == 1);

    }

    public function testMetaData()
    {
        $bucket = $this->client->bucket("metatest");

        # Set some meta
        $bucket->newObject("metatest", array("foo" => 'bar'))
            ->setMeta("foo", "bar")->store();

        # Test that we load the meta back
        $object = $bucket->get("metatest");
        $this->_assert($object->getMeta("foo") == "bar");

        # Test that the meta is preserved when we rewrite the object
        $bucket->get("metatest")->store();
        $object = $bucket->get("metatest");
        $this->_assert($object->getMeta("foo") == "bar");

        # Test that we remove meta
        $object->removeMeta("foo")->store();
        $anotherObject = $bucket->get("metatest");
        $this->_assert($anotherObject->getMeta("foo") === null);
    }

    public function testNotHasKey()
    {
        $bucket = $this->client->bucket('bucket');

        $exists = $bucket->hasKey('missing');
        $this->_assert(!$exists);
    }

    public function testHasKey()
    {
        $bucket = $this->client->bucket('bucket');

        $rand = rand();
        $obj = $bucket->newObject('foo', $rand);
        $obj->store();

        $exists = $bucket->hasKey('foo');
        $this->_assert($exists);
    }

    private function _assert($bool)
    {
        $this->assertTrue($bool);
    }
}
