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
 * to <marco.debo.debortoli@gmail.com> so we can send you a copy immediately.
 *
 * @category   Riak
 * @package    LinkPhase
 * @copyright  Copyright (c) 2012 debo <marco.debo.debortoli@gmail.com> (https://github.com/MarcoDeBortoli)
 */
namespace Basho\Riak;

/**
 * LinkPhase
 *
 * @category   Riak
 * @package    LinkPhase
 * @author     debo <marco.debo.debortoli@gmail.com> (https://github.com/MarcoDeBortoli)
 */
class LinkPhase
{
    /**
     * Construct a LinkPhase object.
     * @param string $bucket - The bucket name.
     * @param string $tag - The tag.
     * @param boolean $keep - True to return results of this phase.
     */
    public function __construct($bucket, $tag, $keep)
    {
        $this->bucket = $bucket;
        $this->tag = $tag;
        $this->keep = $keep;
    }

    /**
     * Convert the LinkPhase to an associative array. Used
     * internally.
     */
    public function to_array()
    {
        $stepdef = array("bucket" => $this->bucket,
            "tag" => $this->tag,
            "keep" => $this->keep);
        return array("link" => $stepdef);
    }
}