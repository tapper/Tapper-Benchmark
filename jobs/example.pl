#!/usr/bin/env perl

use strict;
use warnings;

use lib '~/git/TapperModel/lib';
use lib '~/git/Tapper-Benchmark/lib';

BEGIN {
    use CGI::Carp qw(carpout);
    open(my $fh_log, ">>/var/log/tapper_benchmark_test.log") or die("Unable to open mycgi-log: $!\n");
    carpout($fh_log);
}

require JSON::XS;
require YAML::Syck;
require Tapper::Model;
require Tapper::Benchmark;

my $or_bench = Tapper::Benchmark->new({
    dbh    => Tapper::Model::model()->storage->dbh,
    debug  => 1,
    config => YAML::Syck::LoadFile(
        '~/git/Tapper-Benchmark/conf/default-tapper_benchmark.conf'
    )
});

#$or_bench->add_single_benchmark({
#    name => 'testbenchmark',
#    unit => 'einheit',
#    data => [
#        {
#            value          => 123.45,
#            testrun_id     => 123,
#            machine        => 'mx1.small',
#            benchmark_date => '2013-09-25 12:12:00',
#        },{
#            value          => 124.57,
#            testrun_id     => 123,
#            machine        => 'mx1.small',
#            benchmark_date => '2013-09-25 12:02:14',
#        },{
#            value          => 122.88,
#            testrun_id     => 123,
#            machine        => 'mx1.large',
#            benchmark_date => '2013-09-23 13:02:14',
#        },{
#            value          => 128.99,
#            testrun_id     => 123,
#            machine        => 'mx1.large',
#            benchmark_date => '2013-09-23 22:23:14',
#        },{
#            value          => 133.11,
#            testrun_id     => 123,
#            machine        => 'mx1.xlarge',
#            benchmark_date => '2013-09-23 21:43:54',
#        }
#    ],
#});

#$or_bench->add_multi_benchmark([
#    {
#        name           => 'testbenchmark 1',
#        unit           => 'einheit',
#        value          => 123.45,
#        testrun_id     => 123,
#        machine        => 'mx1.small',
#        benchmark_date => '2013-09-25 12:12:00',
#    },{
#        name           => 'testbenchmark 1',
#        unit           => 'einheit',
#        value          => 124.57,
#        testrun_id     => 123,
#        machine        => 'mx1.small',
#        benchmark_date => '2013-09-25 12:02:14',
#    },{
#        name           => 'testbenchmark 2',
#        unit           => 'einheit',
#        value          => 122.88,
#        testrun_id     => 123,
#        machine        => 'mx1.large',
#        benchmark_date => '2013-09-23 13:02:14',
#    },{
#        name           => 'testbenchmark 2',
#        unit           => 'einheit',
#        value          => 128.99,
#        testrun_id     => 123,
#        machine        => 'mx1.large',
#        benchmark_date => '2013-09-23 22:23:14',
#    },{
#        name           => 'testbenchmark 2',
#        unit           => 'einheit',
#        value          => 133.11,
#        testrun_id     => 123,
#        machine        => 'mx1.xlarge',
#        benchmark_date => '2013-09-23 21:43:54',
#    }
#]);

require Data::Dumper;
#warn "1 ----------------------------------------------------";
#warn Data::Dumper::Dumper(
#    $or_bench->search_array({
#        select      => [
#            'testrun_id',
#            'machine',
#            'bench_date',
#        ],
#        where       => [
#            ['!=','machine'   ,'mx1.small'],
#            ['=' ,'bench'     ,'testbenchmark 1'],
#            ['=' ,'bench_unit','einheit'],
#        ],
#        limit       => 2,
#        order_by     => [
#            'bench_date',
#        ],
#    })
#);
#warn "2 ----------------------------------------------------";
#warn Data::Dumper::Dumper(
#    $or_bench->search_array({
#        select      => [
#            'testrun_id',
#            'machine',
#        ],
#        where       => [
#            ['!=','machine','mx1.small'],
#            ['=','bench','testbenchmark'],
#        ],
#        limit       => 2,
#        offset      => 1,
#    })
#);
#warn "3 ----------------------------------------------------";
#warn Data::Dumper::Dumper(
#    $or_bench->search_array({
#        select      => [
#            [ 'avg', 'bench_value' ],
#            'testrun_id',
#            'machine',
#        ],
#        where  => [
#            ['=','bench','testbenchmark'],
#        ],
#    })
#);
#warn "4 ----------------------------------------------------";
#warn Data::Dumper::Dumper(
#    $or_bench->search_array({
#        select      => [
#            'testrun_id',
#            'machine',
#        ],
#        order_by    => [
#            'machine',
#            [ 'bench_value', 'DESC' ],
#            ['=','bench','testbenchmark'],
#        ],
#        limit       => 4,
#        offset      => 1,
#    })
#);
#warn "5 ----------------------------------------------------";
#warn Data::Dumper::Dumper(
#    $or_bench->search_hash({
#        keys        => [
#            'testrun_id',
#            'machine',
#            'bench_value_id',
#        ],
#        select      => [
#            'testrun_id',
#            'machine',
#        ],
#        where       => [
#            ['!=','machine','mx1.small'],
#            ['=','bench','testbenchmark'],
#        ],
#    })
#);

#$or_bench->extrapolate({
#    extrapolation_type  => 'month',
#    exclude_additionals => [qw/ benchmark_date /],
#    date_from           => '2013-01-01 00:00:00',
#    date_to             => '2014-01-01 00:00:00',
#});

exit;