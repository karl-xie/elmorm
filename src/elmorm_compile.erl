-module(elmorm_compile).

-include("elmorm.hrl").

-export([parse_file/1]).
-export([parse_binary/1]).
-export([type_default_len/2]).

parse_file(FileName) ->
    case file:read_file(FileName) of
    {ok, Binary} ->
        parse_binary(Binary);
    {error, Reason} ->
        {error, Reason}
    end.

parse_binary(Binary) ->
    List = unicode:characters_to_list(Binary),
    case elmorm_scanner:string(List) of
    {ok, Tokens, _Line} ->
        case elmorm_parser:parse(Tokens) of
        {ok, Parsed} ->
            case collect_tables(Parsed) of
            {ok, Statements, Tables} ->
                apply_other_statement(Statements, Tables);
            {error, Error} ->
                {error, Error}
            end;
        {error, Error} ->
            {error, Error}
        end;
    {error, Error, Line} ->
        {error, {Error, Line}}
    end.

collect_tables(Parsed) ->
    collect_table(Parsed, [], []).

collect_table([], Statements, Tables) -> {ok, lists:reverse(Statements), lists:reverse(Tables)};
collect_table([{table, Name, TableOpts, TableDefinds} | T], Statements, Tables) ->
    case collect_table_opts(TableOpts) of
    {ok, Options} ->
        case collect_table_defs(TableDefinds) of
        {ok, FieldL, IndexL, PrimaryL} ->
            LenOfPrimary = length(PrimaryL),
            case LenOfPrimary > 1 of
            true -> 
                {error, {Name, {too_many_primary_key, PrimaryL}}};
            false ->
                Table = #elm_table{
                    name = erlang:list_to_binary(Name),
                    options = Options,
                    fields = FieldL,
                    primary_key = PrimaryL,
                    index = IndexL
                },
                collect_table(T, Statements, [Table | Tables])
            end;
        {error, Error} ->
            {error, {Name, Error}}
        end;
    {error, Error} ->
        {error, {Name, Error}}
    end;
collect_table([{alter, Name, Operates} | T], Statements, Tables) ->
    case collect_alter_def(Operates, Name, []) of
    {ok, SubOper} ->
        Stat = #elm_alter{
            table = erlang:list_to_binary(Name),
            op_list = SubOper
        },
        collect_table(T, [Stat | Statements], Tables);
    {error, Error} ->
        {error, Error}
    end;
collect_table([{drop_table, _Names} | T], Statements, Tables) ->
    collect_table(T, Statements, Tables);
collect_table([{set, _Scope, _VarName, _Value} | T], Statements, Tables) ->
    collect_table(T, Statements, Tables);
collect_table([{set_charset, _Charset, _Collate} | T], Statements, Tables) ->
    collect_table(T, Statements, Tables);
collect_table([_ | T], Statements, Tables) ->
    collect_table(T, Statements, Tables).

collect_table_opts(TableOpts) ->
    collect_table_opt(TableOpts, ?TABLE_OPTS).
collect_table_opt([], Opts) -> {ok, Opts};
collect_table_opt([{charset, Charset} | T], Opts) ->
    collect_table_opt(T, Opts#{charset => Charset});
collect_table_opt([{collate, Collate} | T], Opts) ->
    collect_table_opt(T, Opts#{collate => Collate});
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
    {error, {unrecognized_table_opt, H}}.

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
    case decode_data_type(DataType) of
        {ok, {DType, DLen, Charset, IsSigned}} ->
            case collect_column_opts(ColOptions) of
            {ok, Options} ->
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
            {error, Error} ->
                {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end;
collect_table_def([{idx, NameAndType, IdxParts, IdxOptions} | T], Map) ->
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
    case collect_index_parts(IdxParts, Cols) of
    {ok, Fields} ->
        case collect_index_opts(IdxOptions) of
        {ok, Options} ->
            Index = #elm_index{
                seq = IdxSeq,
                name = Name,
                class = normal,
                index_type = IdxType,
                fields = Fields,
                options = Options
            },
            collect_table_def(T, Map#{idx_seq => IdxSeq + 1, indexs => [Index | Indexs], idx_name => NIName});
        {error, Error} ->
            {error, Error}
        end;
    {error, Error} ->
        {error, Error}
    end;
collect_table_def([{primary, IdxType, IdxParts} | T], Map) ->
    #{pri_seq := PSeq, pri := Primary, cols := Cols} = Map,
    case collect_index_parts(IdxParts, Cols) of
    {ok, Fields} ->
        Index = #elm_index{
            seq = PSeq,
            name = undefined,
            class = primary,
            index_type = IdxType,
            fields = Fields
        },
        collect_table_def(T, Map#{pri_seq => PSeq + 1, pri => [Index | Primary]});
    {error, Error} ->
        {error, Error}
    end;
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
    case collect_index_parts(IdxParts, Cols) of
    {ok, Fields} ->
        Index = #elm_index{
            seq = IdxSeq,
            name = Name,
            class = unique,
            index_type = IdxType,
            fields = Fields
        },
        collect_table_def(T, Map#{idx_seq => IdxSeq + 1, indexs => [Index | Indexs], idx_name => NIName});
    {error, Error} ->
        {error, Error}
    end;
collect_table_def([H | _], _Map) ->
    {error, {unrecognized_table_defind, H}}.

collect_alter_def([], _Name, Operates) -> {ok, lists:reverse(Operates)};
collect_alter_def([{add, ColDef, Seq} | T], Name, Operates) ->
    case collect_table_defs([ColDef]) of
    {ok, [Column], [], []} ->
        Op = #elm_alter_op{
            method = add,
            field = Column,
            opt_seq = Seq
        },
        collect_alter_def(T, Name, [Op | Operates]);
    {error, Error} ->
        {error, {Name, Error}}
    end;
collect_alter_def([{modify, ColDef, Seq} | T], Name, Operates) ->
    case collect_table_defs([ColDef]) of
    {ok, [Column], [], []} ->
        Op = #elm_alter_op{
            method = modify,
            old_col_name = Column#elm_field.name,
            field = Column,
            opt_seq = Seq
        },
        collect_alter_def(T, Name, [Op | Operates]);
    {error, Error} ->
        {error, {Name, Error}}
    end;
collect_alter_def([{change, OldColName, ColDef, Seq} | T], Name, Operates) ->
    case collect_table_defs([ColDef]) of
    {ok, [Column], [], []} ->
        Op = #elm_alter_op{
            method = change,
            old_col_name = OldColName,
            field = Column,
            opt_seq = Seq
        },
        collect_alter_def(T, Name, [Op | Operates]);
    {error, Error} ->
        {error, {Name, Error}}
    end;
collect_alter_def([{drop, ColName} | T], Name, Operates) ->
    Op = #elm_alter_op{
        method = drop,
        old_col_name = erlang:list_to_binary(ColName),
        opt_seq = undefined
    },
    collect_alter_def(T, Name, [Op | Operates]).

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

decode_data_type({tinyint, Len, Signed}) -> {ok, {tinyint, Len, undefined, Signed}};
decode_data_type({smallint, Len, Signed}) -> {ok, {smallint, Len, undefined, Signed}};
decode_data_type({mediumint, Len, Signed}) -> {ok, {mediumint, Len, undefined, Signed}};
decode_data_type({int, Len, Signed}) -> {ok, {int, Len, undefined, Signed}};
decode_data_type({bigint, Len, Signed}) -> {ok, {bigint, Len, undefined, Signed}};
decode_data_type({varchar, Len, Charset}) -> {ok, {varchar, Len, Charset, undefined}};
decode_data_type({char, Len, Charset}) -> {ok, {char, Len, Charset, undefined}};
decode_data_type(tinyblob) -> {ok, {tinyblob, undefined, undefined, undefined}};
decode_data_type(blob) -> {ok, {blob, undefined, undefined, undefined}};
decode_data_type(mediumblob) -> {ok, {mediumblob, undefined, undefined, undefined}};
decode_data_type(longblob) -> {ok, {longblob, undefined, undefined, undefined}};
decode_data_type(text) -> {ok, {text, undefined, undefined, undefined}};
decode_data_type(Other) -> {error, {unrecognized_data_type, Other}}.

collect_column_opts(ColOptions) ->
    collect_column_opt(ColOptions, ?COLUMN_OPTS).
collect_column_opt([], Opts) -> {ok, Opts};
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
    {error, {unrecognized_column_option, H}}.

collect_index_parts(IdxParts, Cols) ->
    collect_index_part(IdxParts, Cols, 1, []).
collect_index_part([], _Cols, _Seq, Result) -> {ok, lists:reverse(Result)};
collect_index_part([{Name, Len, Sort} | T], Cols, Seq, Result) ->
    BName = erlang:list_to_binary(Name),
    case lists:keyfind(BName, #elm_field.name, Cols) of
    false ->
        {error, {index_column_not_found, Name}};
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

collect_index_opts(IdxOptions) ->
    collect_index_opt(IdxOptions, ?INDEX_OPTS).
collect_index_opt([], Opts) -> {ok, Opts};
collect_index_opt([{key_block_size, Size} | T], Opts) when is_integer(Size) ->
    collect_index_opt(T, Opts#{key_block_size => Size});
collect_index_opt([{parser, Parser} | T], Opts) when is_list(Parser) ->
    collect_index_opt(T, Opts#{parser => unicode:characters_to_binary(Parser)});
collect_index_opt([{engine_attribute, EngineAttr} | T], Opts) when is_list(EngineAttr) ->
    collect_index_opt(T, Opts#{engine_attribute => unicode:characters_to_binary(EngineAttr)});
collect_index_opt([{secondary_engine_attribute, SecondaryEngineAttr} | T], Opts) when is_list(SecondaryEngineAttr) ->
    collect_index_opt(T, Opts#{secondary_engine_attribute => unicode:characters_to_binary(SecondaryEngineAttr)});
collect_index_opt([{visible, Visible} | T], Opts) ->
    collect_index_opt(T, Opts#{visible => Visible});
collect_index_opt([{invisible, InVisible} | T], Opts) ->
    collect_index_opt(T, Opts#{invisible => InVisible});
collect_index_opt([{using, IndexType} | T], Opts) ->
    collect_index_opt(T, Opts#{using => IndexType});
collect_index_opt([{comment, Comment} | T], Opts) when is_list(Comment) ->
    collect_index_opt(T, Opts#{comment => unicode:characters_to_binary(Comment)});
collect_index_opt([H | _], _Opts) ->
    {error, {unrecognized_index_option, H}}.

apply_other_statement([], Result) ->
    {ok, Result};
apply_other_statement([#elm_alter{} = H | T], Result) ->
    case apply_alter(H, Result) of
    {ok, NResult} ->
        apply_other_statement(T, NResult);
    {error, Error} ->
        {error, Error}
    end;
apply_other_statement([_ | T], Result) ->
    apply_other_statement(T, Result).

apply_alter(Alter, Tables) ->
    #elm_alter{table = TableName, op_list = OpList} = Alter,
    case lists:keyfind(TableName, #elm_table.name, Tables) of
    #elm_table{} = ElmTable ->
        case apply_alter_op_loop(OpList, ElmTable) of
        {ok, NElmTable} ->
            NTables = lists:keyreplace(TableName, #elm_table.name, Tables, NElmTable),
            {ok, NTables};
        {error, Error} ->
            {error, Error}
        end;
    false ->
        {error, {table_not_found, TableName}}
    end.

apply_alter_op_loop([], ElmTable) -> {ok, ElmTable};
apply_alter_op_loop([#elm_alter_op{method = add} = Op | T], ElmTable) ->
    #elm_table{name = TableName, fields = Fields} = ElmTable,
    case insert_field(Fields, Op) of
    {ok, NFields} ->
        NElmTable = ElmTable#elm_table{fields = NFields},
        apply_alter_op_loop(T, NElmTable);
    false ->
        io:format("11111111 ElmTable is ~p~n", [ElmTable]),
        io:format("11111111 Op is ~p~n", [Op]),
        {error, {add_column_fail, TableName, Op#elm_alter_op.field#elm_field.name}}
    end;
apply_alter_op_loop([#elm_alter_op{method = modify} = Op | T], ElmTable) ->
    #elm_table{name = TableName, fields = Fields} = ElmTable,
    case modify_field(Fields, Op) of
    {ok, NFields} ->
        NElmTable = ElmTable#elm_table{fields = NFields},
        apply_alter_op_loop(T, NElmTable);
    false ->
        io:format("22222222 ElmTable is ~p~n", [ElmTable]),
        io:format("22222222 Op is ~p~n", [Op]),
        {error, {modify_column_fail, TableName, Op#elm_alter_op.field#elm_field.name}}
    end;
apply_alter_op_loop([#elm_alter_op{method = change} = Op | T], ElmTable) ->
    #elm_table{name = TableName, fields = Fields} = ElmTable,
    case modify_field(Fields, Op) of
    {ok, NFields} ->
        NElmTable = ElmTable#elm_table{fields = NFields},
        apply_alter_op_loop(T, NElmTable);
    false ->
        io:format("33333333 ElmTable is ~p~n", [ElmTable]),
        io:format("33333333 Op is ~p~n", [Op]),
        {error, {change_column_fail, TableName, Op#elm_alter_op.field#elm_field.name}}
    end;
apply_alter_op_loop([#elm_alter_op{method = drop} = Op | T], ElmTable) ->
    #elm_table{name = TableName, fields = Fields} = ElmTable,
    case drop_field(Fields, Op) of
    {ok, NFields} ->
        NElmTable = ElmTable#elm_table{fields = NFields},
        apply_alter_op_loop(T, NElmTable);
    false ->
        {error, {drop_column_fail, TableName, Op#elm_alter_op.old_col_name}}
    end.


insert_field(Fields, H) ->
    case H#elm_alter_op.opt_seq of
    first ->
        AField = H#elm_alter_op.field#elm_field{seq = 1, pre_col_name = undefined},
        case Fields of
        [OriFirst | T] ->
            NOriFirst = OriFirst#elm_field{
                seq = OriFirst#elm_field.seq + 1,
                pre_col_name = AField#elm_field.name
            },
            {ok, [AField, NOriFirst | [X#elm_field{seq = X#elm_field.seq + 1} || X <- T]]};
        [] ->
            {ok, [AField]}
        end;
    undefined ->
        case Fields of
        [] ->
            AField = H#elm_alter_op.field#elm_field{seq = 1, pre_col_name = undefined},
            {ok, [AField]};
        _ ->
            PreCol = lists:last(Fields),
            AField = H#elm_alter_op.field#elm_field{
                seq = PreCol#elm_field.seq + 1,
                pre_col_name = PreCol#elm_field.name
            },
            {ok, Fields ++ [AField]}
        end;
    {'after', Name} ->
        case split_fields(Fields, Name, []) of
        {ok, PreFields, PreCol, SufFields} ->
            AField = H#elm_alter_op.field#elm_field{
                seq = PreCol#elm_field.seq + 1,
                pre_col_name = PreCol#elm_field.name
            },
            case SufFields of
            [FirstSuff | T] ->
                NFirstSuff = FirstSuff#elm_field{
                    seq = FirstSuff#elm_field.seq + 1,
                    pre_col_name = AField#elm_field.name
                },
                {ok, PreFields ++ [PreCol, AField, NFirstSuff | [X#elm_field{seq = X#elm_field.seq + 1} || X <- T]]};
            [] ->
                {ok, PreFields ++ [PreCol, AField]}
            end;
        false ->
            false
        end
    end.

modify_field(Fields, H) ->
    case split_fields(Fields, H#elm_alter_op.old_col_name, []) of
    {ok, PreFields, OldCol, SuffFields} ->
        case H#elm_alter_op.opt_seq of
        first ->
            AField = H#elm_alter_op.field#elm_field{seq = 1, pre_col_name = undefined},
            case SuffFields of
            [FirstSuff | T] ->
                NFirstSuff = FirstSuff#elm_field{pre_col_name = OldCol#elm_field.pre_col_name},
                NPreFields = [X#elm_field{seq = X#elm_field.seq + 1} || X <- PreFields],
                {ok, [AField | NPreFields] ++ [NFirstSuff | T]};
            [] ->
                NPreFields = [X#elm_field{seq = X#elm_field.seq + 1} || X <- PreFields],
                {ok, [AField | NPreFields]}
            end;
        undefined ->
            AField = H#elm_alter_op.field#elm_field{
                seq = OldCol#elm_field.seq,
                pre_col_name = OldCol#elm_field.pre_col_name
            },
            case SuffFields of
            [FirstSuff | T] ->
                NFirstSuff = FirstSuff#elm_field{pre_col_name = AField#elm_field.name},
                {ok, PreFields ++ [AField, NFirstSuff | T]};
            [] ->
                {ok, PreFields ++ [AField]}
            end;
        {'after', Name} ->
            case split_fields(PreFields, Name, []) of
            {ok, PPreFields, PCol, PSuffFields} ->
                AField = H#elm_alter_op.field#elm_field{
                    seq = PCol#elm_field.seq + 1,
                    pre_col_name = PCol#elm_field.name
                },
                case PSuffFields of
                [PFirstSuff | T] ->
                    NPFirstSuff = PFirstSuff#elm_field{
                        seq = PFirstSuff#elm_field.seq + 1,
                        pre_col_name = H#elm_alter_op.field#elm_field.name
                    },
                    NPSuffFields = [NPFirstSuff | [X#elm_field{seq = X#elm_field.seq + 1} || X <- T]],
                    case SuffFields of
                    [FirstSuff | T2] ->
                        NFirstSuff = FirstSuff#elm_field{pre_col_name = OldCol#elm_field.pre_col_name},
                        {ok, PPreFields ++ [PCol, AField | NPSuffFields] ++ [NFirstSuff | T2]};
                    [] ->
                        {ok, PPreFields ++ [PCol, AField | NPSuffFields]}
                    end;
                [] ->
                    {ok, PPreFields ++ [PCol, AField]}
                end;
            false ->
                case split_fields(SuffFields, Name, []) of
                {ok, SPreFields, SCol, SSuffFields} ->
                    AField = H#elm_alter_op.field#elm_field{
                        seq = SCol#elm_field.seq,
                        pre_col_name = SCol#elm_field.name
                    },
                    NSCol = SCol#elm_field{seq = SCol#elm_field.seq - 1},
                    case SPreFields of
                    [SFirstPre | T] ->
                        NSFirstPre = SFirstPre#elm_field{
                            seq = SFirstPre#elm_field.seq - 1,
                            pre_col_name = OldCol#elm_field.pre_col_name
                        },
                        NSPreFields = [NSFirstPre | [X#elm_field{seq = X#elm_field.seq - 1} || X <- T]],
                        case SSuffFields of
                        [SSuffFirst | T2] ->
                            NSSuffFirst = SSuffFirst#elm_field{pre_col_name = AField#elm_field.name},
                            {ok, PreFields ++ NSPreFields ++ [NSCol, AField, NSSuffFirst | T2]};
                        [] ->
                            {ok, PreFields ++ NSPreFields ++ [NSCol, AField]}
                        end;
                    [] ->
                        case SSuffFields of
                        [SSuffFirst | T2] ->
                            NSSuffFirst = SSuffFirst#elm_field{pre_col_name = AField#elm_field.name},
                            {ok, PreFields ++ [NSCol, AField, NSSuffFirst | T2]};
                        [] ->
                            {ok, PreFields ++ [NSCol, AField]}
                        end
                    end;
                false ->
                    false
                end
            end
        end;
    false ->
        false
    end.

drop_field(Fields, H) ->
    case split_fields(Fields, H#elm_alter_op.field#elm_field.name, []) of
    {ok, PreFields, OldCol, SuffFields} ->
        case SuffFields of
        [FirstSuff | T] ->
            NFirstSuff = FirstSuff#elm_field{
                seq = FirstSuff#elm_field.seq - 1,
                pre_col_name = OldCol#elm_field.pre_col_name
            },
            {ok, PreFields ++ [NFirstSuff | [X#elm_field{seq = X#elm_field.seq - 1} || X <- T]]};
        [] ->
            {ok, PreFields}
        end;
    false ->
        false
    end.

split_fields([], _Name, _PreFields) -> false;
split_fields([#elm_field{name = Name} = H | T], Name, PreFields) ->
    {ok, lists:reverse(PreFields), H, T};
split_fields([H | T], Name, PreFields) ->
    split_fields(T, Name, [H | PreFields]).

type_default_len(tinyint, false) -> {ok, 3};
type_default_len(tinyint, _) -> {ok, 4};
type_default_len(smallint, false) -> {ok, 5};
type_default_len(smallint, _) -> {ok, 6};
type_default_len(mediumint, _) -> {ok, 8};
type_default_len(int, false) -> {ok, 10};
type_default_len(int, _) -> {ok, 11};
type_default_len(bigint, _) -> {ok, 20};
type_default_len(_, _) -> false.