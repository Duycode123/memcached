#!/usr/bin/env perl

use strict;
use Test::More tests => 6;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $server = new_memcached();
my $sock = $server->sock;
my $key = "del_key";

print $sock "delete $key\r\n";
is (scalar <$sock>, "NOT_FOUND\r\n", "not found on delete");

print $sock "add $key 0 0 1\r\nx\r\n";
is (scalar <$sock>, "STORED\r\n", "Add before a broken delete.");

print $sock "delete $key 10 noreply\r\n";
# Does not reply
# is (scalar <$sock>, "ERROR\r\n", "Even more invalid delete");

print $sock "delete $key noreply\r\n";
# Will not reply, so let's do a set and check that.

print $sock "set $key 0 0 1\r\nx\r\n";
is (scalar <$sock>, "STORED\r\n", "Stored a key");

print $sock "delete $key\r\n";
is (scalar <$sock>, "DELETED\r\n", "Properly deleted");

print $sock "set $key 0 0 1\r\nx\r\n";
is (scalar <$sock>, "STORED\r\n", "Stored a key");

print $sock "delete $key noreply\r\n";
# will not reply, but a subsequent add will succeed

print $sock "add $key 0 0 1\r\nx\r\n";
is (scalar <$sock>, "STORED\r\n", "Add succeeded after deletion.");

