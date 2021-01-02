-module(elmorm).

-include("elmorm.hrl").

%% API exports
-export([compare/2]).

%%====================================================================
%% API functions
%%====================================================================
compare(NewVersion, OldVersion) when is_binary(NewVersion) andalso is_binary(OldVersion) ->
    case elmorm_compile:parse_binary(NewVersion) of
    {ok, NewTables} ->
        case elmorm_compile:parse_binary(OldVersion) of
        {ok, OldTables} ->
            {ok, elmorm_compare:compare_tables(NewTables, OldTables, ?DEFAULT_OPTIONS)};
        {error, Error} ->
            {error, {old_version, Error}}
        end;
    {error, Error} ->
        {error, {new_version, Error}}
    end.

%%====================================================================
%% Internal functions
%%====================================================================
