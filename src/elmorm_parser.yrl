Nonterminals
k_elmorm 
k_create_ddl k_table_options k_table_nn_options k_table_option k_col_options k_col_option
k_drop_ddl
k_set_dal

k_create_defs k_create_def k_nn_create_defs k_col_def k_data_type k_data_type_int k_data_type_len
    k_data_type_is_signed k_data_type_varchar k_data_type_char k_data_type_text k_data_type_blob 
    k_data_type_charset k_default_val k_not_null_value k_name
%% index define
    k_idx_def k_normal_idx_def k_primary_idx_def k_idx_type k_idx_parts k_idx_part k_idx_sort
    k_unique_idx_def k_idx_name_and_type

k_names

k_var.

Terminals ';' '=' ',' '(' ')' '.' string integer float var name
    create table default charset character set engine comment alias codec
    tinyint smallint int bigint signed unsigned varchar char not null
    storage collate primary key index using tinyblob blob mediumblob longblob
    drop exists if names global local session qualifier text auto_increment unique.

Rootsymbol k_elmorm.

Endsymbol '$end'.

k_elmorm -> '$empty' : [].
k_elmorm -> k_create_ddl k_elmorm : ['$1' | '$2'].
k_elmorm -> k_drop_ddl k_elmorm : ['$1' | '$2'].
k_elmorm -> k_set_dal k_elmorm : ['$1' | '$2'].

k_create_ddl -> create table k_name '(' k_create_defs ')' k_table_options ';' : {table, '$3', '$7', '$5'}.

k_create_defs -> '$empty' : [].
k_create_defs -> k_create_def : ['$1'].
k_create_defs -> k_create_def ',' k_nn_create_defs : ['$1' | '$3'].

k_nn_create_defs -> k_create_def : ['$1'].
k_nn_create_defs -> k_create_def ',' k_nn_create_defs : ['$1' | '$3'].

k_create_def -> k_col_def : '$1'.
k_create_def -> k_idx_def : '$1'.

k_col_def -> k_name k_data_type k_col_options: {col, '$1', '$2', '$3'}.

%% data type 
k_data_type -> k_data_type_int : '$1'.
k_data_type -> k_data_type_varchar : '$1'.
k_data_type -> k_data_type_char : '$1'.
k_data_type -> k_data_type_text : '$1'.
k_data_type -> k_data_type_blob : '$1'.

k_data_type_int -> tinyint k_data_type_len k_data_type_is_signed : {tinyint, '$2', '$3'}.
k_data_type_int -> smallint k_data_type_len k_data_type_is_signed : {smallint, '$2', '$3'}.
k_data_type_int -> int k_data_type_len k_data_type_is_signed : {int, '$2', '$3'}.
k_data_type_int -> bigint k_data_type_len k_data_type_is_signed : {bigint, '$2', '$3'}.

k_data_type_varchar -> varchar '(' integer ')' k_data_type_charset: {varchar, unwrap('$3'), '$5'}.
k_data_type_char -> char k_data_type_len k_data_type_charset : {char, '$2', '$3'}.

k_data_type_text -> text : text.

k_data_type_blob -> tinyblob : tinyblob.
k_data_type_blob -> blob : blob.
k_data_type_blob -> mediumblob : mediumblob.
k_data_type_blob -> longblob : longblob.

k_data_type_charset -> '$empty' : undefined.
k_data_type_charset -> character set var : unwrap('$3').

k_data_type_len -> '$empty' : undefined.
k_data_type_len -> '(' integer ')' : unwrap('$2').

k_data_type_is_signed -> '$empty' : undefined.
k_data_type_is_signed -> signed : true.
k_data_type_is_signed -> unsigned : false.

%% column options
k_col_options -> '$empty' : [].
k_col_options -> k_col_option k_col_options : ['$1' | '$2'].

k_col_option -> null : {null, true}.
k_col_option -> not null : {null, false}.
k_col_option -> default k_default_val : {default, '$2'}.
k_col_option -> comment string : {comment, unwrap('$2')}.
k_col_option -> storage var : {storage, unwrap('$2')}.
k_col_option -> collate var : {collate, unwrap('$2')}.
k_col_option -> auto_increment : {auto_increment, true}.

%% index 
k_idx_def -> k_normal_idx_def : '$1'.
k_idx_def -> k_primary_idx_def : '$1'.
k_idx_def -> k_unique_idx_def : '$1'.

k_normal_idx_def -> index k_idx_name_and_type '(' k_idx_parts ')' : {idx, '$2', '$4'}.
k_normal_idx_def -> key k_idx_name_and_type '(' k_idx_parts ')' : {idx, '$2', '$4'}.

k_primary_idx_def -> primary key k_idx_type '(' k_idx_parts ')' : {primary, '$3', '$5'}.

k_unique_idx_def -> unique k_idx_name_and_type '(' k_idx_parts ')' : {unique_idx, '$2', '$4'}.
k_unique_idx_def -> unique index k_idx_name_and_type '(' k_idx_parts ')' : {unique_idx, '$3', '$5'}.
k_unique_idx_def -> unique key k_idx_name_and_type '(' k_idx_parts ')' : {unique_idx, '$3', '$5'}.

k_idx_name_and_type -> '$empty' : [].
k_idx_name_and_type -> k_name : [{name, '$1'}].
k_idx_name_and_type -> using var : [{type, unwrap('$2')}].
k_idx_name_and_type -> k_name using var : [{name, '$1'}, {type, '$3'}].

k_idx_type -> '$empty' : undefined.
k_idx_type -> using var : unwrap('$2').

k_idx_parts -> k_idx_part : ['$1'].
k_idx_parts -> k_idx_part ',' k_idx_parts : ['$1' | '$3'].

k_idx_part -> k_name k_data_type_len k_idx_sort : {'$1', '$2', '$3'}.
k_idx_sort -> '$empty' : undefined.
k_idx_sort -> var : unwrap('$1').

%% table options
k_table_options -> '$empty' : [].
k_table_options -> k_table_option k_table_nn_options : ['$1' | '$2'].
k_table_options -> k_table_option ',' k_table_nn_options : ['$1' | '$3'].

k_table_nn_options -> k_table_option : ['$1'].
k_table_nn_options -> k_table_option k_table_nn_options : ['$1' | '$2'].
k_table_nn_options -> k_table_option ',' k_table_nn_options : ['$1' | '$3'].

k_table_option -> default character set '=' k_var : {charset, '$5'}.
k_table_option -> default charset '=' k_var : {charset, '$4'}.
k_table_option -> engine '=' k_var : {engine, '$3'}.
k_table_option -> comment '=' k_var : {comment, '$3'}.
k_table_option -> alias '=' k_var : {alias, '$3'}.
k_table_option -> codec '=' k_var : {codec, '$3'}.
k_table_option -> auto_increment '=' integer : {auto_increment, unwrap('$3')}.

k_drop_ddl -> drop table if exists k_names ';' : {drop_table, '$5'}.
k_drop_ddl -> drop table k_names ';' : {drop_table, '$3'}.

%% set statement
k_set_dal -> set global var '=' k_not_null_value ';' : {set, global, unwrap('$3'), '$5'}.
k_set_dal -> set qualifier global '.' var '=' k_not_null_value ';' : {set, global, unwrap('$5'), '$7'}.
k_set_dal -> set session var '=' k_not_null_value ';' : {set, session, unwrap('$3'), '$5'}.
k_set_dal -> set local var '=' k_not_null_value ';' : {set, session, unwrap('$3'), '$5'}.
k_set_dal -> set qualifier session '.' var '=' k_not_null_value ';' : {set, session, unwrap('$5'), '$7'}.
k_set_dal -> set qualifier local '.' var '=' k_not_null_value ';' : {set, session, unwrap('$5'), '$7'}.
k_set_dal -> set qualifier var '=' k_not_null_value ';' : {set, session, unwrap('$3'), '$5'}.
k_set_dal -> set var '=' k_not_null_value ';' : {set, session, unwrap('$2'), '$4'}.
k_set_dal -> set names var ';' : {set_charset, unwrap('$3'), default}.
k_set_dal -> set names var collate var ';' : {set_charset, unwrap('$3'), unwrap('$5')}.

k_names -> k_name : ['$1'].
k_names -> k_name ',' k_names : ['$1' | '$3'].

k_name -> name : unwrap('$1').
k_name -> var : unwrap('$1').

k_default_val -> string : unwrap('$1').
k_default_val -> integer : unwrap('$1').
k_default_val -> float : unwrap('$1').
k_default_val -> null : null.

k_not_null_value -> string : unwrap('$1').
k_not_null_value -> integer : unwrap('$1').
k_not_null_value -> float : unwrap('$1').

k_var -> string : unwrap('$1').
k_var -> integer : unwrap('$1').
k_var -> float : unwrap('$1').
k_var -> var : unwrap('$1').

Erlang code.
unwrap({_, _, V}) -> V;
unwrap({V, _}) -> V.