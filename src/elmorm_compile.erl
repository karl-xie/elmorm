-module(elmorm_compile).

-include("elmorm.hrl").

-export([foo/0]).
-export([parse_file/1]).

foo() ->
    parse_file("./test.kkk").

parse_file(FileName) ->
    case file:read_file(FileName) of
    {ok, Binary} ->
        List = unicode:characters_to_list(Binary),
        case elmorm_scanner:string(List) of
        {ok, Tokens, _Line} ->
            case elmorm_parser:parse(Tokens) of
            {ok, Parsed} ->
                collect_tables(Parsed);
            {error, Error} ->
                {error, Error}
            end;
        {error, Error, Line} ->
            {error, {Error, Line}}
        end;
    {error, Reason} ->
        {error, Reason}
    end.

collect_tables(Parsed) ->
    {ok, collect_table(Parsed, [])}.

collect_table([], Result) -> lists:reverse(Result);
collect_table([{table, Name, TableOpts, TableDefinds} | T], Result) ->
    Options = collect_table_opts(TableOpts),
    {ok, FieldL, IndexL, PrimaryL} = collect_table_defs(TableDefinds),
    case PrimaryL of
    [] -> skip;
    [_] -> skip;
    _ -> exit({too_many_primary_key, PrimaryL})
    end,
    Table = #elm_table{
        name = erlang:list_to_binary(Name),
        options = Options,
        fields = FieldL,
        primary_key = PrimaryL,
        index = IndexL
    },
    collect_table(T, [Table | Result]);
collect_table([{drop_table, _Names} | T], Result) ->
    collect_table(T, Result);
collect_table([{set, _Scope, _VarName, _Value} | T], Result) ->
    collect_table(T, Result);
collect_table([{set_charset, _Charset, _Collate} | T], Result) ->
    collect_table(T, Result);
collect_table([_ | T], Result) ->
    collect_table(T, Result).

collect_table_opts(TableOpts) ->
    collect_table_opt(TableOpts, ?TABLE_OPTS).
collect_table_opt([], Opts) -> Opts;
collect_table_opt([{charset, Charset} | T], Opts) ->
    collect_table_opt(T, Opts#{charset => Charset});
collect_table_opt([{engine, Engine} | T], Opts) ->
    collect_table_opt(T, Opts#{engine => Engine});
collect_table_opt([{comment, Comment} | T], Opts) when is_list(Comment) ->
    collect_table_opt(T, Opts#{comment => unicode:characters_to_binary(Comment)});
collect_table_opt([{alias, Alias} | T], Opts) when is_list(Alias) ->
    collect_table_opt(T, Opts#{alias => list_to_binary(Alias)});
collect_table_opt([{codec, Codec} | T], Opts) when is_list(Codec) ->
    collect_table_opt(T, Opts#{codec => Codec});
collect_table_opt([{auto_increment, AutoInc} | T], Opts) when is_integer(AutoInc) ->
    collect_table_opt(T, Opts#{auto_increment => AutoInc});
collect_table_opt([H | _T], _Opts) ->
    exit({unrecognized_table_opt, H}).


collect_table_defs(TableDefinds) ->
    Map = #{
        col_seq => 1, cols => [], 
        idx_seq => 1, indexs => [], 
        pri_seq => 1, pri => [],
        idx_name => #{}
    },
    collect_table_def(TableDefinds, Map).
collect_table_def([], Map) ->
    #{cols := Cols, indexs := Indexs, pri := Primary, idx_name := IName} = Map,
    Indexs2 = pretreat_index_name(lists:reverse(Indexs), IName),
    {ok, lists:reverse(Cols), Indexs2, lists:reverse(Primary)};
collect_table_def([{col, Name, DataType, ColOptions} | T], Map) ->
    #{col_seq := ColSeq, cols := Cols} = Map,
    {DType, DLen, Charset, IsSigned} = decode_data_type(DataType),
    Options = collect_column_opts(ColOptions),
    case Cols of
    [] -> PreColName = undefined;
    [PreCol | _] -> PreColName = PreCol#elm_field.name
    end,
    Col = #elm_field{
        seq = ColSeq,
        name = erlang:list_to_binary(Name),
        pre_col_name = PreColName,
        data_type = DType,
        data_len = DLen,
        charset = Charset,
        is_signed = IsSigned,
        options = Options
    },
    collect_table_def(T, Map#{col_seq => ColSeq + 1, cols => [Col | Cols]});
collect_table_def([{idx, NameAndType, IdxParts} | T], Map) ->
    #{idx_seq := IdxSeq, indexs := Indexs, cols := Cols, idx_name := IName} = Map,
    case proplists:get_value(name, NameAndType, undefined) of
    undefined -> 
        Name = undefined,
        NIName = IName;
    Other -> 
        Name = erlang:list_to_binary(Other),
        NIName = maps:put(Name, true, IName)
    end,
    IdxType = proplists:get_value(type, NameAndType, undefined),
    Fields = collect_index_parts(IdxParts, Cols),
    Index = #elm_index{
        seq = IdxSeq,
        name = Name,
        class = normal,
        index_type = IdxType,
        fields = Fields
    },
    collect_table_def(T, Map#{idx_seq => IdxSeq + 1, indexs => [Index | Indexs], idx_name => NIName});
collect_table_def([{primary, IdxType, IdxParts} | T], Map) ->
    #{pri_seq := PSeq, pri := Primary, cols := Cols} = Map,
    Fields = collect_index_parts(IdxParts, Cols),
    Index = #elm_index{
        seq = PSeq,
        name = undefined,
        class = primary,
        index_type = IdxType,
        fields = Fields
    },
    collect_table_def(T, Map#{pri_seq => PSeq + 1, pri => [Index | Primary]});
collect_table_def([{unique_idx, NameAndType, IdxParts} | T], Map) ->
    #{idx_seq := IdxSeq, indexs := Indexs, cols := Cols, idx_name := IName} = Map,
    case proplists:get_value(name, NameAndType, undefined) of
    undefined -> 
        Name = undefined,
        NIName = IName;
    Other -> 
        Name = erlang:list_to_binary(Other),
        NIName = maps:put(Name, true, IName)
    end,
    IdxType = proplists:get_value(type, NameAndType, undefined),
    Fields = collect_index_parts(IdxParts, Cols),
    Index = #elm_index{
        seq = IdxSeq,
        name = Name,
        class = unique,
        index_type = IdxType,
        fields = Fields
    },
    collect_table_def(T, Map#{idx_seq => IdxSeq + 1, indexs => [Index | Indexs], idx_name => NIName});
collect_table_def([H | _], _Map) ->
    exit({unrecognized_table_defind, H}).

pretreat_index_name(Indexs, NameMap) ->
    pretreat_index_name(Indexs, NameMap, []).
pretreat_index_name([], _NameMap, R) -> lists:reverse(R);
pretreat_index_name([H | T], NameMap, R) ->
    case is_binary(H#elm_index.name) of
    true -> pretreat_index_name(T, NameMap, [H | R]);
    false ->
        FirstCol = hd(H#elm_index.fields),
        {ok, Name} = calc_index_name(FirstCol#elm_index_field.col_name, NameMap),
        NNameMap = maps:put(Name, true, NameMap),
        pretreat_index_name(T, NNameMap, [H#elm_index{name = Name} | R])
    end.

calc_index_name(FirstColName, M) ->
    case maps:is_key(FirstColName, M) of
    true -> calc_index_name(FirstColName, 2, M); %% start with 2
    false -> {ok, FirstColName}
    end.
calc_index_name(FCN, Num, M) ->
    Name = iolist_to_binary([FCN, "_", erlang:integer_to_binary(Num)]),
    case maps:is_key(Name, M) of
    true -> calc_index_name(FCN, Num + 1, M);
    false -> {ok, Name}
    end.

decode_data_type({tinyint, Len, Signed}) -> {tinyint, Len, undefined, Signed};
decode_data_type({smallint, Len, Signed}) -> {smallint, Len, undefined, Signed};
decode_data_type({int, Len, Signed}) -> {int, Len, undefined, Signed};
decode_data_type({bigint, Len, Signed}) -> {bigint, Len, undefined, Signed};
decode_data_type({varchar, Len, Charset}) -> {varchar, Len, Charset, undefined};
decode_data_type({char, Len, Charset}) -> {char, Len, Charset, undefined};
decode_data_type(tinyblob) -> {tinyblob, undefined, undefined, undefined};
decode_data_type(blob) -> {blob, undefined, undefined, undefined};
decode_data_type(mediumblob) -> {mediumblob, undefined, undefined, undefined};
decode_data_type(longblob) -> {longblob, undefined, undefined, undefined};
decode_data_type(text) -> {text, undefined, undefined, undefined};
decode_data_type(Other) -> exit({unrecognized_data_type, Other}).

collect_column_opts(ColOptions) ->
    collect_column_opt(ColOptions, ?COLUMN_OPTS).
collect_column_opt([], Opts) -> Opts;
collect_column_opt([{null, Nullable} | T], Opts) ->
    collect_column_opt(T, Opts#{null => Nullable});
collect_column_opt([{default, Default} | T], Opts) when is_list(Default) ->
    collect_column_opt(T, Opts#{default => unicode:characters_to_binary(Default)});
collect_column_opt([{default, Default} | T], Opts) when is_integer(Default) ->
    collect_column_opt(T, Opts#{default => Default});
collect_column_opt([{default, null} | T], Opts) ->
    collect_column_opt(T, Opts#{default => null});
collect_column_opt([{comment, Comment} | T], Opts) when is_list(Comment) ->
    collect_column_opt(T, Opts#{comment => unicode:characters_to_binary(Comment)});
collect_column_opt([{storage, Storage} | T], Opts) ->
    collect_column_opt(T, Opts#{storage => Storage});
collect_column_opt([{collate, Collate} | T], Opts) ->
    collect_column_opt(T, Opts#{collate => Collate});
collect_column_opt([{auto_increment, AutoInc} | T], Opts) ->
    collect_column_opt(T, Opts#{auto_increment => AutoInc});
collect_column_opt([H | _], _Opts) ->
    exit({unrecognized_column_option, H}).

collect_index_parts(IdxParts, Cols) ->
    collect_index_part(IdxParts, Cols, 1, []).
collect_index_part([], _Cols, _Seq, Result) -> lists:reverse(Result);
collect_index_part([{Name, Len, Sort} | T], Cols, Seq, Result) ->
    BName = erlang:list_to_binary(Name),
    case lists:keyfind(BName, #elm_field.name, Cols) of
    false ->
        exit({index_column_not_found, Name});
    #elm_field{seq = ColSeq} ->
        E = #elm_index_field{
            seq = Seq,
            col_seq = ColSeq,
            col_name = BName,
            len = Len,
            sort = Sort
        },
        collect_index_part(T, Cols, Seq + 1, [E | Result])
    end.
