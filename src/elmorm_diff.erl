-module(elmorm_diff).

-include("elmorm.hrl").

-export([diff/2]).
-export([calc_column_operate/2]).

diff(TableA, TableB) ->
    {ok, TableOptDiff} = calc_table_options(TableA, TableB),
    {ok, ColDrops, ColAdds, ColModifys} = calc_column_operate(TableA, TableB),
    {ok, IndexDiff} = calc_index_operate(TableA, TableB),
    Map1 = #{
        table_opt_diff => TableOptDiff,
        col_remove => ColDrops,
        col_add => ColAdds,
        col_modify => ColModifys
    },
    Map = maps:merge(Map1, IndexDiff),
    {ok, Map}.

calc_table_options(TableA, TableB) ->
    L1 = maps:to_list(TableB#elm_table.options),
    {_Rest, Change} =
    lists:foldl(fun
    ({Key, undefined}, {InAOptions, InChange}) ->
        %% can not remove tablel optinos 
        {maps:remove(Key, InAOptions), InChange}; 
    ({Key, ValueB}, {InAOptions, InChange}) ->
        case maps:get(Key, InAOptions) of
        ValueB ->
            {maps:remove(Key, InAOptions), InChange};
        _Other ->
            {maps:remove(Key, InAOptions), maps:put(Key, ValueB, InChange)}
        end
    end, {TableA#elm_table.options, #{}}, L1),
    {ok, Change}.


%% get the operates of transform A to B
calc_column_operate(TableA, TableB) ->
    #elm_table{fields = FieldsA0} = TableA,
    #elm_table{fields = FieldsB0} = TableB,
    FieldsA = [X#elm_field{seq = 0} || X <- FieldsA0],
    FieldsB = [X#elm_field{seq = 0} || X <- FieldsB0],
    {ok, Drops, RestFieldsA} = drop_columns(FieldsA, FieldsB),
    {ok, Adds} = add_columns(RestFieldsA, FieldsB),
    {ok, Modifys} = modify_columns(RestFieldsA, FieldsB, Adds),
    {ok, Drops, Adds, Modifys}.

drop_columns(FieldsA, FieldsB) ->
    drop_columns(FieldsA, FieldsB, [], []).
drop_columns([], _FieldsB, Drops, Rest) ->
    {ok, lists:reverse(Drops), lists:reverse(Rest)};
drop_columns([H | T], FieldsB, Drops, Rest) ->
    case lists:keyfind(H#elm_field.name, #elm_field.name, FieldsB) of
    false ->
        drop_columns(T, FieldsB, [H | Drops], Rest);

    _ ->
        drop_columns(T, FieldsB, Drops, [H | Rest])
    end.

add_columns(FieldsA, FieldsB) ->
    add_columns(FieldsB, FieldsA, []).
add_columns([], _FieldsA, Add) -> {ok, lists:reverse(Add)};
add_columns([H | T], FieldsA, Add) ->
    case lists:keymember(H#elm_field.name, #elm_field.name, FieldsA) of
    true -> add_columns(T, FieldsA, Add);
    false -> add_columns(T, FieldsA, [H | Add])
    end.

modify_columns(FieldsA, FieldsB, Add) ->
    FieldsA1 = lists:foldl(fun(X, InAcc) ->
        insert_field(X, InAcc)
    end, FieldsA, Add),
    F = fun(X, InAcc) ->
        elmorm_dlink:append(X#elm_field.name, InAcc)
    end,
    DLA = lists:foldl(F, elmorm_dlink:empty(), FieldsA1),
    DLB = lists:foldl(F, elmorm_dlink:empty(), FieldsB),
    Standard = init_standard(FieldsB),
    InitScore = calc_score(DLA, Standard),
    {ok, modify_columns_loop(FieldsB, FieldsA1, elmorm_dlink:head(DLA), DLA, DLB, InitScore, Standard, [])}.

modify_columns_loop(FieldsB, FieldsA, _Cur, _DLA, _DLB, 0, _Standard, R) ->
    NewR = 
    lists:foldl(fun(X, InAcc) ->
        Old = lists:keyfind(X#elm_field.name, #elm_field.name, FieldsA),
        case Old#elm_field{seq = 0, pre_col_name = <<>>} =:= X#elm_field{seq = 0, pre_col_name = <<>>} of
        true -> InAcc;
        false -> [X | InAcc]
        end
    end, R, FieldsB),
    lists:reverse(NewR);
modify_columns_loop([#elm_field{name = Cur} = H | T], FieldsA, Cur, DLA, DLB, Score, Standard, R) ->
    OldH = lists:keyfind(Cur, #elm_field.name, FieldsA),
    case OldH#elm_field{seq = 0, pre_col_name = <<>>} =:= H#elm_field{seq = 0, pre_col_name = <<>>} of
    true ->
        {ok, Next} = elmorm_dlink:next(Cur, DLA),
        modify_columns_loop(T, FieldsA, Next, DLA, DLB, Score, Standard, R);
    false ->
        {ok, Next} = elmorm_dlink:next(Cur, DLA),
        modify_columns_loop(T, FieldsA, Next, DLA, DLB, Score, Standard, [H | R])
    end;
modify_columns_loop([H | T], FieldsA, Cur, DLA, DLB, _Score, Standard, R) ->
    #elm_field{name = Name, pre_col_name = PreColName} = H,
    %% op-1 move H to here
    DLA_OP1_1 = elmorm_dlink:delete(Name, DLA),
    DLA_OP1_2 = elmorm_dlink:insert(Name, PreColName, DLA_OP1_1),
    Score_OP1 = calc_score(DLA_OP1_2, Standard),

    %% op-2 move Cur to right
    {ok, Next_OP2} = elmorm_dlink:next(Cur, DLA),
    CurFieldOP2 = lists:keyfind(Cur, #elm_field.name, T),
    DLA_OP2_1 = elmorm_dlink:delete(Cur, DLA),
    DLA_OP2_2 = elmorm_dlink:insert(Cur, CurFieldOP2#elm_field.pre_col_name, DLA_OP2_1),
    Score_OP2 = calc_score(DLA_OP2_2, Standard),

    case Score_OP1 =< Score_OP2 of
    true -> %% use op-1
        {ok, NewCur} = elmorm_dlink:next(Name, DLA_OP1_2),
        modify_columns_loop(T, FieldsA, NewCur, DLA_OP1_2, DLB, Score_OP1, Standard, [H | R]);
    false -> %% use op-2
        modify_columns_loop([H | T], FieldsA, Next_OP2, DLA_OP2_2, DLB, Score_OP2, Standard, [CurFieldOP2 | R])
    end.

init_standard(Fields) ->
    {_, OutMap} =
    lists:foldl(fun(Field, {InIndex, InMap}) ->
        {InIndex + 1, maps:put(Field#elm_field.name, InIndex, InMap)}
    end, {1, #{}}, Fields),
    OutMap.

calc_score(DLA, StandardM) ->
    {_, Score} =
    lists:foldl(fun(Name, {InIndex, InScore}) ->
        V = maps:get(Name, StandardM),
        {InIndex + 1, erlang:abs(V - InIndex) + InScore}
    end, {1, 0}, elmorm_dlink:to_list(DLA)),
    Score.

insert_field(#elm_field{pre_col_name = undefined} = Field, L) ->
    [Field | L];
insert_field(Field, L) ->
    insert_field(L, Field, []).
insert_field([], Field, _R) ->
    exit({unexpect_field_seq, Field});
insert_field([#elm_field{name = Name} = H | T], #elm_field{pre_col_name = Name} = Field, R) ->
    lists:reverse([H | R]) ++ [Field | T];
insert_field([H | T], Field, R) ->
    insert_field(T, Field, [H | R]).

%% index diff
calc_index_operate(TableA, TableB) ->
    #elm_table{
        primary_key = PriKeyA,
        index = IdxA
    } = TableA,
    #elm_table{
        primary_key = PriKeyB,
        index = IdxB
    } = TableB,
    {ok, PriKeyRemove, PriKeyAdd} = diff_index(PriKeyA, PriKeyB),
    {ok, IdxRemove, IdxAdd} = diff_index(IdxA, IdxB),
    {ok, #{
        prikey_remove => PriKeyRemove, 
        prikey_add => PriKeyAdd,
        idx_remove => IdxRemove,
        idx_add => IdxAdd
    }}.

diff_index(IndexLA, IndexLB) ->
    Remove = IndexLA -- IndexLB,
    Add = IndexLB -- IndexLA,
    {ok, Remove, Add}.

