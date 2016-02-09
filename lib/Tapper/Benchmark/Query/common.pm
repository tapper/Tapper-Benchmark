package Tapper::Benchmark::Query::common;
# ABSTRACT: Tapper::Benchmark - querying - backend base class

use strict;
use warnings;
use base 'Tapper::Benchmark::Query';

use List::MoreUtils qw( any );

my %h_used_selects;
my %h_default_columns = (
    'NAME'      => 'b.bench',
    'UNIT'      => 'bu.bench_unit',
    'VALUE'     => 'bv.bench_value',
    'VALUE_ID'  => 'bv.bench_value_id',
    'CREATED'   => 'bv.created_at',
);

sub _NOW { "CURRENT_TIMESTAMP" }

sub _FOR_UPDATE { "FOR UPDATE" }

sub default_columns {
    return %h_default_columns;
}

sub benchmark_operators {
    return ( '=', '!=', 'like', 'not like', '<', '>', '<=', '>=' );
}

sub create_where_clause {

    my ( $or_self, $s_column_name, $ar_value ) = @_;

    my $s_where_clause = q##;
    if ( $ar_value->[0] eq 'not like' ) {
        $s_where_clause = "$s_column_name NOT LIKE ?";
    }
    elsif ( $ar_value->[0] eq 'like' ) {
        $s_where_clause = "$s_column_name LIKE ?";
    }
    elsif (
           $ar_value->[0] eq '<'
        || $ar_value->[0] eq '>'
        || $ar_value->[0] eq '<='
        || $ar_value->[0] eq '>='
    ) {
        $s_where_clause = "$s_column_name $ar_value->[0] ?";
    }
    elsif ( $ar_value->[0] eq '=' ) {
        if ( $#{$ar_value} > 1 ) {
            $s_where_clause = "$s_column_name IN (" . (join ',', map {'?'} 2..@{$ar_value}) . ')';
        }
        else {
            $s_where_clause = "$s_column_name = ?";
        }
    }
    elsif ( $ar_value->[0] eq '!=' ) {
        if ( $#{$ar_value} > 1 ) {
            $s_where_clause = "$s_column_name NOT IN (" . (join ',', map {'?'} 2..@{$ar_value}) . ')';
        }
        else {
            $s_where_clause = "$s_column_name != ?";
        }
    }
    else {
        require Carp;
        Carp::confess("unknown operator '$ar_value->[0]'");
        return;
    }

    return $s_where_clause;

}

sub create_period_check {

    my ( $or_self, $s_column, $dt_from, $dt_to ) = @_;

    my @a_vals;
    my $s_where;
    if ( $dt_from ) {
        if ( my ( $s_date, $s_time ) = $dt_from =~ /(\d{4}-\d{2}-\d{2})( \d{2}:\d{2}:\d{2})?/ ) {
            $s_where .= "\nAND $s_column > ?";
            push @a_vals, $s_date . ( $s_time || ' 00:00:00' );
        }
        else {
            require Carp;
            Carp::confess(q#unknown date format for 'date_from'#);
            return;
        }
    }
    if ( $dt_to ) {
        if ( my ( $s_date, $s_time ) = $dt_to =~ /(\d{4}-\d{2}-\d{2})( \d{2}:\d{2}:\d{2})?/ ) {
            $s_where .= "\nAND $s_column < ?";
            push @a_vals, $s_date . ( $s_time || ' 23:59:59' );
        }
        else {
            require Carp;
            Carp::confess(q#unknown date format for 'date_to'#);
            return;
        }
    }

    return {
        vals  => \@a_vals,
        where => $s_where,
    };

}

sub select_benchmark_point_essentials {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT
          b.bench,
          bv.bench_value,
          bu.bench_unit
        FROM
          $or_self->{config}{tables}{benchmark_table} b
        JOIN
          $or_self->{config}{tables}{benchmark_value_table} bv
          ON
            b.bench_id = bv.bench_id
        LEFT JOIN
          $or_self->{config}{tables}{unit_table} bu
          ON
            b.bench_unit_id = bu.bench_unit_id
        WHERE
          bv.bench_value_id = ?
        ;
    ", @a_vals );

}

sub select_complete_benchmark_point {

    my ( $or_self, @a_vals ) = @_;

    my $query = "
        SELECT
          bat.bench_additional_type,
          bav.bench_additional_value
        FROM
          benchs b
        JOIN
          bench_values bv
          ON
            b.bench_id = bv.bench_id
        JOIN
          bench_additional_type_relations batr
          ON
            bv.bench_id = batr.bench_id
        JOIN
          bench_additional_types bat
          ON
            batr.bench_additional_type_id = bat.bench_additional_type_id
        JOIN
          bench_additional_relations bar
          ON
            bv.bench_value_id = bar.bench_value_id
        JOIN
          bench_additional_values bav
          ON
            bar.bench_additional_value_id = bav.bench_additional_value_id AND
            bat.bench_additional_type_id  = bav.bench_additional_type_id
        WHERE
          bv.bench_value_id = ?
        ORDER BY
          bat.bench_additional_type";
    return $or_self->execute_query( $query, @a_vals );
}

sub select_addtype_by_name {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT bench_additional_type_id
        FROM $or_self->{config}{tables}{additional_type_table}
        WHERE bench_additional_type = ?
    ", @a_vals );

}

sub select_min_subsume_type {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT bench_subsume_type_id
        FROM $or_self->{config}{tables}{subsume_type_table}
        ORDER BY bench_subsume_type_rank ASC
        LIMIT 1
    " );

}

sub select_subsume_type {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT
            bench_subsume_type_id,
            bench_subsume_type_rank,
            datetime_strftime_pattern
        FROM
            $or_self->{config}{tables}{subsume_type_table}
        WHERE
            bench_subsume_type = ?
    ", @a_vals );

}

sub select_check_subsumed_values {

    my ( $or_self, $hr_vals ) = @_;

    if (! $hr_vals->{subsume_type_id} ) {
        require Carp;
        Carp::confess(q#required parameter 'subsume_type_id' is missing#);
        return;
    }

    my $hr_period_check = $or_self->create_period_check(
        'bv.created_at', $hr_vals->{date_from}, $hr_vals->{date_to}
    );

    return $or_self->execute_query(
        "
            SELECT
                bv.bench_value_id
            FROM
                bench_values bv
                JOIN bench_subsume_types bet
                    ON ( bv.bench_subsume_type_id = bet.bench_subsume_type_id )
            WHERE
                bet.bench_subsume_type_rank > (
                    SELECT beti.bench_subsume_type_rank
                    FROM bench_subsume_types beti
                    WHERE bench_subsume_type_id = ?
                )
                $hr_period_check->{where}
            LIMIT
                1
        ",
        $hr_vals->{subsume_type_id},
        @{$hr_period_check->{vals}},
    );

}

sub select_data_values_for_subsume {

    my ( $or_self, $hr_vals ) = @_;

    my $hr_period_check = $or_self->create_period_check(
        'bv.created_at', $hr_vals->{date_from}, $hr_vals->{date_to}
    );

    my @a_addexclude_vals;
    my $s_addexclude_where = q##;
    if ( $hr_vals->{exclude_additionals} && @{$hr_vals->{exclude_additionals}} ) {
        $s_addexclude_where = 'AND bav.bench_additional_type_id NOT IN (' . (join ',', map {'?'} @{$hr_vals->{exclude_additionals}}) . ')';
        push @a_addexclude_vals, @{$hr_vals->{exclude_additionals}};
    }

    return $or_self->execute_query(
        "
            SELECT
                b.bench_id,
                bv.bench_value_id,
                bv.created_at,
                bv.bench_value,
                bet.bench_subsume_type_rank,
                GROUP_CONCAT(
                        bav.bench_additional_type_id,
                        '|',
                        bav.bench_additional_value_id
                    ORDER BY
                        bav.bench_additional_type_id,
                        bav.bench_additional_value_id
                    SEPARATOR
                        '-'
                ) AS additionals
            FROM
                benchs b
                JOIN bench_values bv
                    ON ( bv.bench_id = b.bench_id )
                JOIN bench_subsume_types bet
                    ON ( bet.bench_subsume_type_id = bv.bench_subsume_type_id )
                LEFT JOIN (
                    bench_additional_relations bar
                    JOIN bench_additional_values bav
                        ON ( bav.bench_additional_value_id = bar.bench_additional_value_id )
                )
                    ON (
                        bar.active = 1
                        AND bar.bench_value_id = bv.bench_value_id
                        $s_addexclude_where
                    )
            WHERE
                b.active = 1
                AND bv.active = 1
                $hr_period_check->{where}
            GROUP BY
                bet.bench_subsume_type_rank,
                b.bench_id,
                bv.created_at,
                bv.bench_value,
                bv.bench_value_id
            ORDER BY
                b.bench_id,
                additionals,
                bv.created_at
        ",
        @a_addexclude_vals,
        @{$hr_period_check->{vals}},
    );
}

sub select_benchmark {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT bench_id
        FROM $or_self->{config}{tables}{benchmark_table}
        WHERE bench = ?
    ", @a_vals );

}

sub select_benchmark_names {

    my ( $or_self, @a_vals ) = @_;

    my $query = "
        SELECT DISTINCT bench
        FROM $or_self->{config}{tables}{benchmark_table}";
    $query .= "
        WHERE bench LIKE ? " if @a_vals;
    return $or_self->execute_query( $query, @a_vals );

}

sub select_unit {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT bench_unit_id
        FROM $or_self->{config}{tables}{unit_table}
        WHERE bench_unit = ?
    ", @a_vals );

}

sub select_addtype {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT bench_additional_type_id
        FROM $or_self->{config}{tables}{additional_type_table}
        WHERE bench_additional_type = ?
    ", @a_vals );

}

sub select_addvalue {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT bench_additional_value_id
        FROM $or_self->{config}{tables}{additional_value_table}
        WHERE bench_additional_type_id = ? AND bench_additional_value = ?
    ", @a_vals );

}

sub select_addtyperelation {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT bench_id, bench_additional_type_id, created_at
        FROM $or_self->{config}{tables}{additional_type_relation_table}
        WHERE bench_id = ? AND bench_additional_type_id = ?
    ", @a_vals );

}

sub insert_raw_bench_bundle {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        INSERT INTO raw_bench_bundles
            (raw_bench_bundle_serialized)
        VALUES ( ? )
    ", @a_vals );

}

sub copy_benchmark_backup_value {

    my ( $or_self, $hr_vals ) = @_;

    for my $s_param (qw/ new_bench_value_id old_bench_value_id /) {
        if (! $hr_vals->{$s_param} ) {
            require Carp;
            Carp::confess("missing parameter '$s_param'");
            return;
        }
    }

    return $or_self->execute_query( "
        INSERT INTO $or_self->{config}{tables}{benchmark_backup_value_table}
            ( bench_value_id, bench_id, bench_subsume_type_id, bench_value, active, created_at )
        SELECT
            ?, bench_id, bench_subsume_type_id, bench_value, active, created_at
        FROM
            $or_self->{config}{tables}{benchmark_value_table}
        WHERE
            bench_value_id = ?
    ", @{$hr_vals}{qw/ new_bench_value_id old_bench_value_id /} );

}

sub copy_benchmark_backup_additional_relations {

    my ( $or_self, $hr_vals ) = @_;

    for my $s_param (qw/ new_bench_value_id old_bench_value_id /) {
        if (! $hr_vals->{$s_param} ) {
            require Carp;
            Carp::confess("missing parameter '$s_param'");
            return;
        }
    }

    return $or_self->execute_query( "
        INSERT INTO $or_self->{config}{tables}{backup_additional_relation_table}
            ( bench_backup_value_id, bench_additional_value_id, active, created_at )
        SELECT
            ?, bench_additional_value_id, active, created_at
        FROM
            $or_self->{config}{tables}{additional_relation_table}
        WHERE
            bench_value_id = ?
    ", @{$hr_vals}{qw/ new_bench_value_id old_bench_value_id /} );

}

sub update_benchmark_backup_value {

    my ( $or_self, $hr_vals ) = @_;

    return $or_self->execute_query( "
        UPDATE $or_self->{config}{tables}{benchmark_backup_value_table}
        SET bench_value_id = ?
        WHERE bench_value_id = ?
    ", @{$hr_vals}{qw/
        new_bench_value_id
        old_bench_value_id
    /} );

}

sub start_processing_raw_bench_bundle {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        UPDATE raw_bench_bundles
        SET processing = 1
        WHERE raw_bench_bundle_id = ?
    ", @a_vals );

}

sub update_raw_bench_bundle_set_processed {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        UPDATE raw_bench_bundles
        SET processed=1,
            processing=0
        WHERE raw_bench_bundle_id = ?
    ", @a_vals );

}

sub delete_benchmark_additional_relations {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        DELETE FROM $or_self->{config}{tables}{additional_relation_table}
        WHERE bench_value_id = ?
    ", @a_vals );

}

sub delete_benchmark_value {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        DELETE FROM $or_self->{config}{tables}{benchmark_value_table}
        WHERE bench_value_id = ?
    ", @a_vals );

}

# Garbage Collection
sub delete_processed_raw_bench_bundles {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        DELETE FROM raw_bench_bundles
        WHERE processed=1 AND
              processing=0
    ", @a_vals );

}

sub copy_additional_values {

    my ( $or_self, $hr_vals ) = @_;

    for my $s_param (qw/ new_bench_value_id old_bench_value_id /) {
        if (! $hr_vals->{$s_param} ) {
            require Carp;
            Carp::confess("missing parameter '$s_param'");
            return;
        }
    }

    return $or_self->execute_query( "
        INSERT INTO $or_self->{config}{tables}{additional_relation_table}
            ( bench_value_id, bench_additional_value_id, active, created_at )
        SELECT
            ?, bench_additional_value_id, 1, @{[$or_self->_NOW]}
        FROM
            $or_self->{config}{tables}{additional_relation_table}
        WHERE
            bench_value_id = ?
    ", @{$hr_vals}{qw/ new_bench_value_id old_bench_value_id /} );

}

sub select_raw_bench_bundle_for_lock {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT raw_bench_bundle_id
        FROM raw_bench_bundles
        WHERE processed=0 AND
              processing=0
        ORDER BY raw_bench_bundle_id ASC
        LIMIT 1
        @{[$or_self->_FOR_UPDATE]}
    ", @a_vals );
}

sub select_raw_bench_bundle_for_processing {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT raw_bench_bundle_serialized
        FROM raw_bench_bundles
        WHERE raw_bench_bundle_id = ?
        LIMIT 1
        @{[$or_self->_FOR_UPDATE]}
    ", @a_vals );
}

1;
