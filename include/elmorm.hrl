-ifndef(ELMORM_H_H).
-define(ELMORM_H_H, true).

-define(NEWLINE, <<"\n">>).
-define(TAB, <<"  ">>).
-define(DEFAULT_OPTIONS, #{newline => ?NEWLINE, tab => ?TAB}).

-record(elm_index_field, {
    seq :: integer(),
    col_name :: binary(),
    len :: integer() | undefined,
    sort :: asc | desc | undefined
}).

-record(elm_index, {
    seq :: integer(),
    name :: binary(),
    class :: primary | normal | unique,
    index_type :: binary() | undefined,
    fields :: [#elm_index_field{}],
    options :: map()
}).

-record(elm_field, {
    seq :: integer(),
    name :: binary(),
    pre_col_name :: binary() | undefined,
    data_type :: atom(),
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
    index :: [#elm_index{}],
    idx_name_map :: #{}
}).

-record(elm_alter_op, {
    method :: atom(),
    old_col_name :: binary(),
    field :: #elm_field{} | #elm_index{} | undefined,
    opt_seq :: atom()
}).

-record(elm_alter, {
    table :: binary(),
    op_list :: [#elm_alter_op{}]
}).

-define(TABLE_OPTS, #{
    charset => undefined,
    collate => undefined,
    engine => "InnoDB",
    comment => undefined,
    alias => undefined,
    codec => undefined,
    auto_increment => undefined
}).

-define(TABLE_OPTS_SEQ, [engine, charset, collate, auto_increment, comment]).

-define(TABLE_OPTS_SNAME, #{
    engine => <<"ENGINE">>,
    charset => <<"DEFAULT CHARSET">>,
    collate => <<"COLLATE">>,
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

-define(INDEX_OPTS, #{
    key_block_size => undefined,
    parser => undefined,
    engine_attribute => undefined,
    secondary_engine_attribute => undefined,
    visible => undefined,
    invisible => undefined,
    using => undefined,
    comment => undefined
}).

-define(INDEX_OPTS_SEQ, [key_block_size, parser, engine_attribute, 
    secondary_engine_attribute, visible, invisible, using, comment]).

-define(INDEX_OPTS_SNAME, #{
    key_block_size => <<"KEY_BLOCK_SIZE">>,
    parser => <<"WITH PARSER">>,
    engine_attribute => <<"ENGINE_ATTRIBUTE">>,
    secondary_engine_attribute => <<"SECONDARY_ENGINE_ATTRIBUTE">>,
    visible => <<"VISIBLE">>,
    invisible => <<"INVISIBLE">>,
    using => <<"USING">>,
    comment => <<"COMMENT">>
}).

-endif.