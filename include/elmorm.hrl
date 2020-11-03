-ifndef(ELMORM_H_H).
-define(ELMORM_H_H, true).

-record(elm_index_field, {
    seq :: integer(),
    col_seq :: integer(),
    col_name :: binary(),
    len :: integer() | undefined,
    sort :: asc | desc | undefined
}).

-record(elm_index, {
    seq :: integer(),
    name :: binary(),
    class :: primary | normal | unique,
    index_type :: binary() | undefined,
    fields :: [#elm_index_field{}]
}).

-record(elm_field, {
    seq :: integer(),
    name :: binary(),
    pre_col_name :: binary() | undefined,
    data_type :: binary(),
    data_len :: integer(),
    charset :: binary(),
    is_signed :: boolean(),
    options :: map()
}).

-record(elm_table, {
    name :: binary(),
    options :: map(),
    fields :: [#elm_field{}],
    primary_key :: [#elm_index{}],
    index :: [#elm_index{}]
}).

-define(TABLE_OPTS, #{
    charset => undefined,
    engine => undefined,
    comment => undefined,
    alias => undefined,
    codec => undefined,
    auto_increment => undefined
}).

-define(TABLE_OPTS_SEQ, [engine, charset, auto_increment, comment]).

-define(TABLE_OPTS_SNAME, #{
    engine => <<"ENGINE">>,
    charset => <<"DEFAULT CHARACTER SET">>,
    comment => <<"COMMENT">>,
    auto_increment => <<"AUTO_INCREMENT">>
}).

-define(COLUMN_OPTS, #{
    null => undefined,
    default => undefined,
    comment => undefined,
    storage => undefined,
    collate => undefined,
    auto_increment => undefined
}).

-define(COLUMN_OPTS_SEQ, [collate, storage, null, default, auto_increment, comment]).

-define(COLUMN_OPTS_SNAME, #{
    null => <<"NULL">>,
    default => <<"DEFAULT">>,
    comment => <<"COMMENT">>,
    storage => <<"STORAGE">>,
    collate => <<"COLLATE">>,
    auto_increment => <<"AUTO_INCREMENT">>
}).

-endif.