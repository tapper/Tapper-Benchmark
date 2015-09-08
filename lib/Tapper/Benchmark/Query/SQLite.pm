package Tapper::Benchmark::Query::SQLite;
# ABSTRACT: Tapper::Benchmark - qerying - SQLite backend

use strict;
use warnings;
use feature 'switch';
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

sub default_columns {
    return %h_default_columns;
}

sub benchmark_operators {
    return ( '=', '!=', 'like', 'not like', '<', '>', '<=', '>=' );
}

sub create_where_clause {

    my ( $s_column_name, $ar_value ) = @_;

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

sub create_select_column {

    my ( $or_self, $ar_select, $i_counter, $b_aggregate_all ) = @_;

    my $s_aggr_func           = q##;
    my ( $s_aggr, $s_column ) = @{$ar_select};
    my $s_return_select       = q##;

    AGGR: {
        given( $s_aggr ) {
            when ( q##   ) {
                # aggregate all columns if a single column is aggregated
                if ( $b_aggregate_all ) {
                    $s_aggr = $or_self->{config}{default_aggregation};
                    redo AGGR;
                }
                $s_return_select = '${COLUMN}';
            }
            when ( 'min' ) {
                $s_return_select = 'MIN( ${COLUMN} )';
            }
            when ( 'max' ) {
                $s_return_select = 'MAX( ${COLUMN} )';
            }
            when ( 'avg' ) {
                $s_return_select = 'AVG( ${COLUMN} )';
            }
            # Geometric Mean, unsupported in SQLite due to lack of EXP(),
            # see http://stackoverflow.com/questions/13190064/how-to-find-power-of-a-number-in-sqlite
            #
            # when ( 'gem' ) {
            #     $s_return_select = 'EXP( SUM( LOG( ${COLUMN} ) ) / COUNT( ${COLUMN} ) )';
            # }
            when ( 'sum' ) {
                $s_return_select = 'SUM( ${COLUMN} )';
            }
            when ( 'cnt' ) {
                $s_return_select = 'COUNT( ${COLUMN} )';
            }
            when ( 'cnd' ) {
                $s_return_select = 'COUNT( DISTINCT ${COLUMN} )';
            }
            default {
                require Carp;
                Carp::confess("unknown aggregate function '$s_aggr'");
                return;
            }
        }
    } # AGGR

    my ( $s_return_column );
    my $s_replace_as = $s_aggr ? $s_aggr . "_$s_column" : $s_column;

    if ( $h_used_selects{$or_self}{$s_replace_as} ) {
        return;
    }
    if ( any { $s_column eq $_  } keys %h_default_columns ) {
        $h_used_selects{$or_self}{$s_replace_as} = $h_default_columns{$s_column};
    }
    else {
        $s_return_column                         = $s_column;
        $h_used_selects{$or_self}{$s_replace_as} = "bav$i_counter.bench_additional_value";
    }

    $s_return_select =~ s/\${COLUMN}/$h_used_selects{$or_self}{$s_replace_as}/g;

    return ( $s_return_column, "$s_return_select AS '$s_replace_as'", );

}

sub create_period_check {

    my ( $s_column, $dt_from, $dt_to ) = @_;

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

sub select_benchmark_values {

    my ( $or_self, $hr_search ) = @_;

    # clear selected columns
    $h_used_selects{$or_self} = {};

    # deep copy hash
    require JSON::XS;
    $hr_search = JSON::XS::decode_json(
        JSON::XS::encode_json( $hr_search )
    );

    my (
        $s_limit,
        $s_offset,
        $s_order_by,
        @a_select,
        @a_from,
        @a_from_vals,
        @a_where,
        @a_where_vals,
    ) = (
        q##,
        q##,
        q##,
    );

    # limit clause
    if ( $hr_search->{limit} ) {
        if ( $hr_search->{limit} =~ /^\d+$/ ) {
            $s_limit = "LIMIT $hr_search->{limit}";
        }
        else {
            require Carp;
            Carp::confess("invalid limit value '$hr_search->{limit}'");
            return;
        }
    }

    # offset clause
    if ( $hr_search->{offset} ) {
        if ( $hr_search->{offset} =~ /^\d+$/ ) {
            $s_offset = "OFFSET $hr_search->{offset}";
        }
        else {
            require Carp;
            Carp::confess("invalid offset value '$hr_search->{offset}'");
            return;
        }
    }

    # where clause
    my $i_counter = 0;
    if ( $hr_search->{where} ) {

        for my $ar_where ( @{$hr_search->{where}} ) {
            if ( any { $ar_where->[1] eq $_  } keys %h_default_columns ) {
                my $s_column = splice( @{$ar_where}, 1, 1 );
                push @a_where, create_where_clause( $h_default_columns{$s_column}, $ar_where );
                push @a_where_vals , @{$ar_where}[1..$#{$ar_where}];
            }
            else {
                my $s_additional_type = splice( @{$ar_where}, 1, 1 );
                my $hr_additional_type = $or_self
                    ->select_addtype_by_name( $s_additional_type )
                    ->fetchrow_hashref()
                ;
                if ( !$hr_additional_type || !$hr_additional_type->{bench_additional_type_id} ) {
                    require Carp;
                    Carp::confess("benchmark additional value '$s_additional_type' not exists");
                    return;
                }
                push @a_from, "
                    JOIN (
                        $or_self->{config}{tables}{additional_relation_table} bar$i_counter
                        JOIN $or_self->{config}{tables}{additional_value_table} bav$i_counter
                            ON ( bav$i_counter.bench_additional_value_id = bar$i_counter.bench_additional_value_id )
                    )
                        ON (
                            bar$i_counter.bench_value_id = bv.bench_value_id
                            AND bav$i_counter.bench_additional_type_id = ?
                        )
                ";
                push @a_from_vals, $hr_additional_type->{bench_additional_type_id};
                push @a_where, create_where_clause( "bav$i_counter.bench_additional_value", $ar_where );
                push @a_where_vals , @{$ar_where}[1..$#{$ar_where}];
                $i_counter++;
            }
        }
    }

    # select clause
    my $b_aggregate_all = 0;
    if ( $hr_search->{select} ) {
        for my $i_counter ( 0..$#{$hr_search->{select}} ) {
            if ( ref $hr_search->{select}[$i_counter] ne 'ARRAY' ) {
                $hr_search->{select}[$i_counter] = ['',$hr_search->{select}[$i_counter]];
            }
            elsif ( !$b_aggregate_all && $hr_search->{select}[$i_counter][0] ne q## ) {
                $b_aggregate_all = 1;
                for my $s_clause (qw/ order_by limit offset /) {
                    if ( $hr_search->{$s_clause} ) {
                        require Carp;
                        Carp::confess("cannot use '$s_clause' with aggregation");
                    }
                }
            }
        }
    }
    push @{$hr_search->{select} ||= []}, map {['',$_]} keys %h_default_columns;

    for my $ar_select ( @{$hr_search->{select}} ) {

        my ( $s_column, $s_select ) = $or_self->create_select_column(
            $ar_select, $i_counter, $b_aggregate_all,
        );

        if ( $s_select ) {

            push @a_select, $s_select;

            if ( $s_column ) {

                my $hr_additional_type = $or_self
                    ->select_addtype_by_name( $s_column )
                    ->fetchrow_hashref()
                ;
                if ( !$hr_additional_type || !$hr_additional_type->{bench_additional_type_id} ) {
                    require Carp;
                    Carp::confess("benchmark additional value '$s_column' not exists");
                    return;
                }

                push @a_from_vals, $hr_additional_type->{bench_additional_type_id};
                push @a_from, "
                    LEFT JOIN (
                        $or_self->{config}{tables}{additional_relation_table} bar$i_counter
                        JOIN $or_self->{config}{tables}{additional_value_table} bav$i_counter
                            ON ( bav$i_counter.bench_additional_value_id = bar$i_counter.bench_additional_value_id )
                    )
                        ON (
                            bar$i_counter.bench_value_id = bv.bench_value_id
                            AND bav$i_counter.bench_additional_type_id = ?
                        )
                ";
                $i_counter++;
            }
        }

    }

    # order_by clause
    if ( $hr_search->{order_by} ) {
        my @a_order_by_possible  = keys %h_default_columns;
        my @a_order_by_direction = qw/ ASC DESC /;
        if ( $hr_search->{select} ) {
            push @a_order_by_possible, map { $_->[1] } @{$hr_search->{select}};
        }
        my @a_order_by;
        for my $order_column ( @{$hr_search->{order_by}} ) {
            if ( ref $order_column ) {
                if ( any { $order_column->[0] eq $_  } @a_order_by_possible ) {
                    if ( any { $order_column->[1] eq $_ } @a_order_by_direction ) {
                        my $s_numeric_cast = q##;
                        if ( $order_column->[2] && $order_column->[2]{numeric} ) {
                            $s_numeric_cast = '0 + ';
                        }
                        if ( any { $order_column->[0] eq $_ } keys %h_default_columns ) {
                            push @a_order_by, "$s_numeric_cast$h_default_columns{$order_column->[0]} $order_column->[1]";
                        }
                        else {
                            push @a_order_by, "$s_numeric_cast$order_column->[0] $order_column->[1]";
                        }
                    }
                    else {
                        require Carp;
                        Carp::confess("unknown order by direction '$order_column->[1]'");
                        return;
                    }
                }
                else {
                    require Carp;
                    Carp::confess("unknown order by column '$order_column->[0]'");
                    return;
                }
            }
            else {
                if ( any { $order_column eq $_ } @a_order_by_possible ) {
                    if ( any { $order_column eq $_ } keys %h_default_columns ) {
                        push @a_order_by, "$h_default_columns{$order_column} ASC";
                    }
                    else {
                        push @a_order_by, "$order_column ASC";
                    }
                }
                else {
                    require Carp;
                    Carp::confess("unknown order by column '$order_column'");
                    return;
                }
            }
        }
        $s_order_by = 'ORDER BY ' . (join ', ', @a_order_by)
    }

    # replace placeholders inside of raw sql where clause
    my $s_raw_where = $hr_search->{where_sql};
    if ( $s_raw_where ) {
        $s_raw_where =~ s/
            \${(.+?)}
        /
            $h_used_selects{$or_self}{$1}
                ? $h_used_selects{$or_self}{$1}
                : die "column '$1' not exists in SELECT clause"
        /gex;
    }

    return $or_self->execute_query(
        "
            SELECT
                " . ( join ",\n", map {"$_"} @a_select ) . "
            FROM
                $or_self->{config}{tables}{benchmark_table} b
                JOIN $or_self->{config}{tables}{benchmark_value_table} bv
                    ON ( bv.bench_id = b.bench_id )
                LEFT JOIN $or_self->{config}{tables}{unit_table} bu
                    ON ( bu.bench_unit_id = b.bench_unit_id )
                " . ( join "\n", @a_from ) . "
            WHERE
                b.active = 1
                AND bv.active = 1
                " .
                ( @a_where      ? join "\n", map { "AND $_" } @a_where  : q## ) .
                ( $s_raw_where  ? " $s_raw_where"                       : q## ) .
            "
            $s_order_by
            $s_limit
            $s_offset
        ",
        @a_from_vals,
        @a_where_vals,
    );

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

    my $hr_period_check = create_period_check(
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

    my $hr_period_check = create_period_check(
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

sub select_raw_bench_bundle_for_lock {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT raw_bench_bundle_id
        FROM raw_bench_bundles
        WHERE processed=0 AND
              processing=0
        ORDER BY raw_bench_bundle_id ASC
        LIMIT 1
    ", @a_vals );
}

sub select_raw_bench_bundle_for_processing {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        SELECT raw_bench_bundle_serialized
        FROM raw_bench_bundles
        WHERE raw_bench_bundle_id = ?
        LIMIT 1
    ", @a_vals );
}

sub insert_addtyperelation {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        INSERT OR IGNORE INTO $or_self->{config}{tables}{additional_type_relation_table}
            ( bench_id, bench_additional_type_id, created_at )
        VALUES
            ( ?, ?, CURRENT_TIMESTAMP )
    ", @a_vals );

}

sub insert_unit {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        INSERT INTO $or_self->{config}{tables}{unit_table}
            ( bench_unit, created_at )
        VALUES
            ( ?, CURRENT_TIMESTAMP )
    ", @a_vals );

}

sub insert_benchmark {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        INSERT INTO $or_self->{config}{tables}{benchmark_table}
            ( bench, bench_unit_id, active, created_at )
        VALUES
            ( ?, ?, 1, CURRENT_TIMESTAMP )
    ", @a_vals );

}

sub insert_benchmark_value {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        INSERT OR IGNORE INTO $or_self->{config}{tables}{benchmark_value_table}
            ( bench_id, bench_subsume_type_id, bench_value, active, created_at )
        VALUES
            ( ?, ?, ?, 1, CURRENT_TIMESTAMP )
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
            ?, bench_additional_value_id, 1, CURRENT_TIMESTAMP
        FROM
            $or_self->{config}{tables}{additional_relation_table}
        WHERE
            bench_value_id = ?
    ", @{$hr_vals}{qw/ new_bench_value_id old_bench_value_id /} );

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

sub insert_addtype {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        INSERT INTO $or_self->{config}{tables}{additional_type_table}
            ( bench_additional_type, created_at )
        VALUES
            ( ?, CURRENT_TIMESTAMP )
    ", @a_vals );

}

sub insert_addvalue {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        INSERT INTO $or_self->{config}{tables}{additional_value_table}
            ( bench_additional_type_id, bench_additional_value, created_at )
        VALUES
            ( ?, ?, CURRENT_TIMESTAMP )
    ", @a_vals );

}

sub insert_addvaluerelation {

    my ( $or_self, @a_vals ) = @_;

    return $or_self->execute_query( "
        INSERT OR IGNORE INTO $or_self->{config}{tables}{additional_relation_table}
            ( bench_value_id, bench_additional_value_id, active, created_at )
        VALUES
            ( ?, ?, 1, CURRENT_TIMESTAMP )
    ", @a_vals );

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
        DELETE FROM $or_self->{config}{tables}{additional_relation_table}
        WHERE bench_value_id = ?
    ", @a_vals );

}

1;
