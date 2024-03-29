-module(elmorm_compare).

-include("elmorm.hrl").

-export([string/2, compare/3, compare_tables/3]).

%% convert s1 to s2
string(S1, S2) ->
    case elmorm_compile:parse_binary(S1) of
    {ok, Tables1} ->
        case elmorm_compile:parse_binary(S2) of
        {ok, Tables2} ->
            Bin = compare_tables(Tables2, Tables1, ?DEFAULT_OPTIONS),
            {ok, Bin};
        {error, Error} ->
            {error, Error}
        end;
    {error, Error} ->
        {error, Error}
    end.

%% convert file1 to file2, output to file3
compare(File1, File2, File3) ->
    case elmorm_compile:parse_file(File1) of
    {ok, Tables1} ->
        case elmorm_compile:parse_file(File2) of
        {ok, Tables2} ->
            Bin = compare_tables(Tables2, Tables1, ?DEFAULT_OPTIONS),
            case file:open(File3, [binary, write]) of
            {ok, IoDevice} ->
                file:write(IoDevice, Bin),
                file:close(IoDevice),
                ok;
            {error, Error} ->
                {error, Error}
            end;
        {error, Error} ->
            {error, Error}
        end;
    {error, Error} ->
        {error, Error}
    end.
compare_tables(TablesA, TablesB, Opts) ->
    compare_tables(TablesA, TablesB, Opts, []).
compare_tables([], TablesB, Opts, R) ->
    #{newline := NewLine} = Opts,
    lists:reverse(lists:foldl(fun(X, InAcc) ->
        [NewLine, NewLine, drop_table(X#elm_table.name) | InAcc]
    end, R, TablesB));
compare_tables([H | T], TablesB, Opts, R) ->
    #{newline := NewLine} = Opts,
    case lists:keytake(H#elm_table.name, #elm_table.name, TablesB) of
    {value, Tuple, Rest} ->
        case table_diff(Tuple, H, Opts) of
        <<>> ->
            compare_tables(T, Rest, Opts, R);
        Diff ->
            compare_tables(T, Rest, Opts, [NewLine, NewLine, Diff | R])
        end;
    false ->
        compare_tables(T, TablesB, Opts, [NewLine, NewLine, create_table(H, Opts) | R])
    end.

drop_table(Name) ->
    iolist_to_binary([<<"DROP TABLE `">>, Name, <<"`;">>]).

table_diff(TableA, TableB, Opts) ->
    TName = TableA#elm_table.name,
    ALTER = <<"ALTER TABLE `", TName/binary, "` ">>,
    {ok, DiffMap} = elmorm_diff:diff(TableA, TableB),
    #{
        table_opt_diff := TableOptDiff,
        col_remove := ColDrops,
        col_add := ColAdds,
        col_modify := ColModifys,
        prikey_remove := PriKeyRemove, 
        prikey_add := PriKeyAdd,
        idx_remove := IdxRemove,
        idx_add := IdxAdd,
        col_change := ColChanges
    } = DiffMap,
    L0 = build_table_options(ALTER, TableOptDiff),
    L1 = [<<ALTER/binary, "CHANGE COLUMN `", XOldName/binary, "` ", (format_table_field(X, true))/binary, ";">> || {XOldName, X} <- ColChanges],
    L2 = [<<ALTER/binary, "DROP COLUMN `", (X#elm_field.name)/binary, "`;">> || X <- ColDrops],
    L3 = [<<ALTER/binary, "ADD COLUMN ", (format_table_field(X, true))/binary, ";">> || X <- ColAdds],
    L4 = [<<ALTER/binary, "MODIFY COLUMN ", (format_table_field(X, true))/binary, ";">> || X <- ColModifys],
    L5 = [<<ALTER/binary, "DROP PRIMARY KEY;">> || _ <- PriKeyRemove],
    L6 = [<<ALTER/binary, "ADD PRIMARY KEY ", (format_key_parts(X#elm_index.fields))/binary, ";">> || X <- PriKeyAdd],
    L7 = [<<ALTER/binary, "DROP INDEX `", (X#elm_index.name)/binary, "`;">> || X <- IdxRemove],
    L8 = [<<ALTER/binary, "ADD ", (format_table_index(X))/binary, ";">> || X <- IdxAdd],
    L = lists:foldl(fun(Y, InAcc) ->
        Y ++ InAcc
    end, [], [L8, L7, L6, L5, L4, L3, L2, L1, L0]),
    connact_table_diff(L, Opts).
connact_table_diff(L, Opts) ->
    connact_table_diff(L, Opts, <<>>).
connact_table_diff([], _Opts, Bin) -> Bin;
connact_table_diff([H], Opts, Bin) ->
    connact_table_diff([], Opts, <<Bin/binary, H/binary>>);
connact_table_diff([H | T], Opts, Bin) ->
    #{newline := NewLine} = Opts,
    connact_table_diff(T, Opts, <<Bin/binary, H/binary, NewLine/binary>>).

build_table_options(ALTER, TableOptDiff) ->
    case build_table_options(?TABLE_OPTS_SEQ, TableOptDiff, <<>>) of
    <<>> -> [];
    Diff ->
        [<<ALTER/binary, Diff/binary, ";">>]
    end.

build_table_options([], _TOD, R) -> R;
build_table_options([H | T], TOD, <<>>) ->
    case maps:get(H, TOD, undefined) of
    undefined -> build_table_options(T, TOD, <<>>);
    Value -> 
        build_table_options(T, TOD, <<(format_table_option(H, Value))/binary>>)
    end;
build_table_options([H | T], TOD, R) ->
    case maps:get(H, TOD, undefined) of
    undefined -> build_table_options(T, TOD, R);
    Value -> 
        build_table_options(T, TOD, <<R/binary, ",", (format_table_option(H, Value))/binary>>)
    end.

create_table(Table, Opts) ->
    Fields = format_table_fields(Table#elm_table.fields),
    PrimaryKey = format_table_pri_key(Table#elm_table.primary_key),
    Index = format_table_indexs(Table#elm_table.index),
    TableDefine = format_table_define(Fields ++ (PrimaryKey ++ Index), Opts),
    TableOpts = format_table_opts(Table#elm_table.options),
    #{newline := NewLine} = Opts,
    iolist_to_binary([
        <<"CREATE TABLE `">>, Table#elm_table.name, <<"` (">>, TableDefine, 
        NewLine, <<")">>, TableOpts, <<";">>
    ]).

%% begin of fields
format_table_fields(Fields) ->
    format_table_fields(Fields, []).
format_table_fields([], R) -> lists:reverse(R);
format_table_fields([H | T], R) ->
    format_table_fields(T, [format_table_field(H, false) | R]).


format_table_field(Field, true) ->
    iolist_to_binary([
        "`", Field#elm_field.name, "` ", format_type(Field), 
        format_column_options(Field#elm_field.options),
        format_column_seq(Field#elm_field.pre_col_name)

    ]);
format_table_field(Field, false) ->
    iolist_to_binary([
        "`", Field#elm_field.name, "` ", format_type(Field), 
        format_column_options(Field#elm_field.options)
    ]).

format_type(Field) ->
    L0 =
    case is_list(Field#elm_field.charset) of
    true -> [<<" CHARACTER SET ">>, Field#elm_field.charset];
    false -> []
    end,
    L1 =
    case Field#elm_field.is_signed of
    undefined -> L0;
    true -> [<<" signed">> | L0];
    false -> [<<" unsigned">> | L0]
    end,
    L2 =
    case is_integer(Field#elm_field.data_len) of
    true -> ["(", erlang:integer_to_binary(Field#elm_field.data_len), ")" | L1];
    false -> 
        case elmorm_compile:type_default_len(Field#elm_field.data_type, Field#elm_field.is_signed) of
        {ok, DefaultLen} -> ["(", erlang:integer_to_binary(DefaultLen), ")" | L1];
        false -> L1
        end
    end,
    iolist_to_binary([erlang:atom_to_binary(Field#elm_field.data_type, utf8) | L2]).

format_column_options(Options) ->
    iolist_to_binary(format_column_options(?COLUMN_OPTS_SEQ, Options, [])).

format_column_options([], _Options, R) -> lists:reverse(R);
format_column_options([auto_increment | T], Options, R) ->
    case maps:get(auto_increment, Options) of
    undefined -> format_column_options(T, Options, R);
    true ->
        NewR = [<<" ", (maps:get(auto_increment, ?COLUMN_OPTS_SNAME))/binary>> | R],
        format_column_options(T, Options, NewR)
    end;
format_column_options([null | T], Options, R) ->
    case maps:get(null, Options) of
    undefined -> format_column_options(T, Options, R);
    true -> 
        NewR = [<<" ", (maps:get(null, ?COLUMN_OPTS_SNAME))/binary>> | R],
        format_column_options(T, Options, NewR);
    false ->
        NewR = [<<" NOT ", (maps:get(null, ?COLUMN_OPTS_SNAME))/binary>> | R],
        format_column_options(T, Options, NewR)
    end;
format_column_options([default = OptName | T], Options, R) ->
    case maps:get(OptName, Options) of
    undefined -> format_column_options(T, Options, R);
    Value when is_integer(Value) ->
        NewR = [iolist_to_binary([" ", maps:get(OptName, ?COLUMN_OPTS_SNAME), " '", format_option_value(Value), "'"]) | R],
        format_column_options(T, Options, NewR);
    Value ->
        NewR = [iolist_to_binary([" ", maps:get(OptName, ?COLUMN_OPTS_SNAME), " ", format_option_value(Value)]) | R],
        format_column_options(T, Options, NewR)
    end;
format_column_options([OptName | T], Options, R) ->
    case maps:get(OptName, Options) of
    undefined -> format_column_options(T, Options, R);
    Value ->
        NewR = [iolist_to_binary([" ", maps:get(OptName, ?COLUMN_OPTS_SNAME), " ", format_option_value(Value)]) | R],
        format_column_options(T, Options, NewR)
    end.

format_column_seq(undefined) -> <<" FIRST">>;
format_column_seq(Name) -> <<" AFTER `", Name/binary, "`">>.
%% end of fields


%% begin of primary key
format_table_pri_key([]) -> [];
format_table_pri_key([H]) ->
    [iolist_to_binary([<<"PRIMARY KEY ">>, format_key_parts(H#elm_index.fields)])].

format_key_parts(IFields) ->
    iolist_to_binary(["(", format_key_parts(IFields, []), ")"]).
format_key_parts([], R) -> lists:reverse(R);
format_key_parts([H], R) ->
    L0 =
    case H#elm_index_field.sort of
    undefined -> [];
    Other -> [" ", Other]
    end,
    L1 =
    case H#elm_index_field.len of
    undefined -> L0;
    Len -> ["(", erlang:integer_to_binary(Len), ")" | L0]
    end,
    format_key_parts([], [iolist_to_binary(["`", H#elm_index_field.col_name, "`" | L1]) | R]);
format_key_parts([H | T], R) ->
    L0 = [", "],
    L1 =
    case H#elm_index_field.sort of
    undefined -> L0;
    Other -> [" ", Other | L0]
    end,
    L2 =
    case H#elm_index_field.len of
    undefined -> L1;
    Len -> ["(", erlang:integer_to_binary(Len), ")" | L1]
    end,
    format_key_parts(T, [iolist_to_binary(["`", H#elm_index_field.col_name, "`" | L2]) | R]).

%% end of primary key


%% begin of index
format_table_indexs(Index) ->
    format_table_indexs(Index, []).
format_table_indexs([], R) -> lists:reverse(R);
format_table_indexs([H | T], R) ->
    format_table_indexs(T, [format_table_index(H) | R]).
format_table_index(#elm_index{class = normal} = H) ->
    L0 = [" ", format_key_parts(H#elm_index.fields)],
    L1 = 
    case H#elm_index.index_type of
    undefined -> L0;
    IndexType -> [" ", "USING ", IndexType | L0]
    end,
    L2 =
    case H#elm_index.name of
    undefined -> L1;
    IndexName -> [" `", IndexName, "`" | L1]
    end,
    iolist_to_binary([["KEY" | L2], format_index_options(H#elm_index.options)]);
format_table_index(#elm_index{class = unique} = H) ->
    L0 = [" ", format_key_parts(H#elm_index.fields)],
    L1 =
    case H#elm_index.index_type of
    undefined -> L0;
    IndexType -> [" ", "USING ", IndexType | L0]
    end,
    L2 =
    case H#elm_index.name of
    undefined -> L1;
    IndexName -> [" `", IndexName, "`" | L1]
    end,
    iolist_to_binary([["UNIQUE KEY" | L2], format_index_options(H#elm_index.options)]).

format_index_options(Options) ->
    iolist_to_binary(format_index_options(?INDEX_OPTS_SEQ, Options, [])).

format_index_options([], _Options, R) -> lists:reverse(R);
format_index_options([key_block_size | T], Options, R) ->
    case maps:get(key_block_size, Options) of
    undefined -> format_index_options(T, Options, R);
    Value ->
        NewR = [<<" ", (maps:get(key_block_size, ?INDEX_OPTS_SNAME))/binary, " = ", (format_option_value(Value))/binary>> | R],
        format_index_options(T, Options, NewR)
    end;
format_index_options([parser | T], Options, R) ->
    case maps:get(parser, Options) of
    undefined -> format_index_options(T, Options, R);
    Value ->
        NewR = [<<" ", (maps:get(parser, ?INDEX_OPTS_SNAME))/binary, " ", (format_option_value(Value))/binary>> | R],
        format_index_options(T, Options, NewR)
    end;
format_index_options([engine_attribute | T], Options, R) ->
    case maps:get(engine_attribute, Options) of
    undefined -> format_index_options(T, Options, R);
    Value ->
        NewR = [<<" ", (maps:get(engine_attribute, ?INDEX_OPTS_SNAME))/binary, " ", (format_option_value(Value))/binary>> | R],
        format_index_options(T, Options, NewR)
    end;
format_index_options([secondary_engine_attribute | T], Options, R) ->
    case maps:get(secondary_engine_attribute, Options) of
    undefined -> format_index_options(T, Options, R);
    Value ->
        NewR = [<<" ", (maps:get(secondary_engine_attribute, ?INDEX_OPTS_SNAME))/binary, " ", (format_option_value(Value))/binary>> | R],
        format_index_options(T, Options, NewR)
    end;
format_index_options([visible | T], Options, R) ->
    case maps:get(visible, Options) of
    undefined -> format_index_options(T, Options, R);
    true ->
        NewR = [<<" ", (maps:get(visible, ?INDEX_OPTS_SNAME))/binary>> | R],
        format_index_options(T, Options, NewR)
    end;
format_index_options([invisible | T], Options, R) ->
    case maps:get(invisible, Options) of
    undefined -> format_index_options(T, Options, R);
    true ->
        NewR = [<<" ", (maps:get(invisible, ?INDEX_OPTS_SNAME))/binary>> | R],
        format_index_options(T, Options, NewR)
    end;
format_index_options([using | T], Options, R) ->
    case maps:get(using, Options) of
    undefined -> format_index_options(T, Options, R);
    Value ->
        NewR = [<<" ", (maps:get(using, ?INDEX_OPTS_SNAME))/binary, " ", (format_option_value(Value))/binary>> | R],
        format_index_options(T, Options, NewR)
    end;
format_index_options([comment | T], Options, R) ->
    case maps:get(comment, Options) of
    undefined -> format_index_options(T, Options, R);
    Value ->
        NewR = [<<" ", (maps:get(comment, ?INDEX_OPTS_SNAME))/binary, " ", (format_option_value(Value))/binary>> | R],
        format_index_options(T, Options, NewR)
    end.
%% end of index

%% begin of table define
format_table_define(TableDefines, Opts) ->
    iolist_to_binary(format_table_define(TableDefines, Opts, [])).
format_table_define([], _Opts, R) -> lists:reverse(R);
format_table_define([H], Opts, R) ->
    #{newline := NewLine, tab := Space} = Opts,
    format_table_define([], Opts, [iolist_to_binary([NewLine, Space, H]) | R]);
format_table_define([H | T], Opts, R) ->
    #{newline := NewLine, tab := Space} = Opts,
    format_table_define(T, Opts, [iolist_to_binary([NewLine, Space, H, <<",">>]) | R]).
%% end of table define

%% begin of table options
format_table_opts(Options) ->
    iolist_to_binary(format_table_opts(?TABLE_OPTS_SEQ, Options, [])).

format_table_opts([], _Options, R) -> lists:reverse(R);
format_table_opts([OptName | T], Options, R) ->
    case maps:get(OptName, Options) of
    undefined ->
        format_table_opts(T, Options, R);
    Value ->
        NewR = [iolist_to_binary([" ", format_table_option(OptName, Value)]) | R],
        format_table_opts(T, Options, NewR)
    end.

format_table_option(OptName, Value) ->
    <<(maps:get(OptName, ?TABLE_OPTS_SNAME))/binary, "=", (format_option_value(Value))/binary>>.

format_option_value(null) ->
    <<"NULL">>;
format_option_value(V) when is_binary(V) ->
    <<"'", V/binary, "'">>;
format_option_value(V) when is_list(V) ->
    unicode:characters_to_binary(V);
format_option_value(V) when is_integer(V) ->
    erlang:integer_to_binary(V).
%% end of table options