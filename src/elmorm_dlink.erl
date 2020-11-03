-module(elmorm_dlink).

-export([empty/0, insert/2, insert/3, append/2, delete/2, to_list/1, foldl/3, 
    lookup/2, head/1, next/2]).


empty() ->
    {nil, #{}}.

insert(K, DL) ->
    insert_1(K, undefined, DL).
insert(K, P, DL) ->
    insert_1(K, P, DL).

insert_1(K, undefined, {nil, M}) ->
    {K, maps:put(K, {nil, nil}, M)};
insert_1(K, undefined, {Head, M}) ->
    {nil, Next} = maps:get(Head, M),
    NewPreHeadNode = {K, Next},
    M1 = maps:put(Head, NewPreHeadNode, M),
    M2 = maps:put(K, {nil, Head}, M1),
    {K, M2};
insert_1(K, Parent, {Head, M}) ->
    case maps:get(Parent, M) of
    {Pre, nil} ->
        NewParentNode = {Pre, K},
        M1 = maps:put(Parent, NewParentNode, M),
        M2 = maps:put(K, {Parent, nil}, M1),
        {Head, M2};
    {Pre, Next} ->
        NewParentNode = {Pre, K},
        M1 = maps:put(Parent, NewParentNode, M),
        {Parent, CNext} = maps:get(Next, M1),
        NewChildNode = {K, CNext},
        M2 = maps:put(Next, NewChildNode, M1),
        M3 = maps:put(K, {Parent, Next}, M2),
        {Head, M3}
    end.

append(K, {nil, M}) ->
    insert_1(K, undefined, {nil, M});
append(K, {Head, M}) ->
    M1 = append(Head, M, nil, K),
    {Head, M1}.

append(nil, M, Pre, K) ->
    {PreParent, nil} = maps:get(Pre, M),
    M1 = maps:put(Pre, {PreParent, K}, M),
    maps:put(K, {Pre, nil}, M1);
append(Pre, M, _, K) ->
    {_, Next} = maps:get(Pre, M),
    append(Next, M, Pre, K).

delete(K, M) ->
    delete_1(K, M).
delete_1(_K, {nil, M}) ->
    {nil, M};
delete_1(Head, {Head, M}) ->
    {nil, Next} = maps:get(Head, M),
    {Head, Next2} = maps:get(Next, M),
    M1 = maps:remove(Head, M),
    M2 = maps:put(Next, {nil, Next2}, M1),
    {Next, M2};
delete_1(K, {Head, M}) ->
    case maps:get(K, M) of
    {Pre, nil} ->
        {PreParent, K} = maps:get(Pre, M),
        M1 = maps:put(Pre, {PreParent, nil}, M),
        {Head, maps:remove(K, M1)};
    {Pre, Next} ->
        {PreParent, K} = maps:get(Pre, M),
        {K, NextChild} = maps:get(Next, M),
        M1 = maps:put(Pre, {PreParent, Next}, M),
        M2 = maps:put(Next, {Pre, NextChild}, M1),
        {Head, maps:remove(K, M2)}
    end.

to_list({nil, _M}) -> [];
to_list({Head, M}) ->
    to_list(Head, M, []).
to_list(nil, _M, L) -> lists:reverse(L);
to_list(K, M, L) ->
    {_Pre, Next} = maps:get(K, M),
    to_list(Next, M, [K | L]).


foldl(F, InAcc, {Head, M}) ->
    foldl(F, InAcc, Head, M).
foldl(_F, InAcc, nil, _M) -> InAcc;
foldl(F, InAcc, K, M) ->
    {Pre, Next} = maps:get(K, M),
    OutAcc = F(K, {Pre, Next}, InAcc),
    foldl(F, OutAcc, Next, M).

lookup(K, {_Head, M}) ->
    case maps:get(K, M, undefined) of
    undefined -> false;
    {P, N} -> {ok, {P, N}}
    end.
    
head({Head, _M}) -> Head.

next(K, DL) ->
    case lookup(K, DL) of
    false -> false;
    {ok, {_P, N}} -> {ok, N}
    end.