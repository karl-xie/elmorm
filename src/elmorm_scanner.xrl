Definitions.
L = [A-Za-z_]
D = [0-9]
F = (\+|-)?[0-9]+\.[0-9]+((E|e)(\+|-)?[0-9+])?
HEX = 0x[0-9A-Fa-f]+
WS = ([\000-\s])
UNICODE = [\x{0000}-\x{FFFF}]
S = [\(\)\[\]\{\};=,.]

Rules.
`{L}({L}|{D})*` : {token, {name, TokenLine, strip(TokenChars, TokenLen)}}.
{L}({L}|{D})* : {token, keyword_or_var(TokenChars, TokenLine)}.
'[^\']*' : {token, {string, TokenLine, strip(TokenChars, TokenLen)}}.
"[^\"]*" : {token, {string, TokenLine, strip(TokenChars, TokenLen)}}.
{S} : {token, {list_to_atom(TokenChars), TokenLine}}.
{WS}+ : skip_token.
(\+|-)?{D}+ : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.
{F} : {token, {float, TokenLine, list_to_float(TokenChars)}}.
{HEX} : {token, {integer, TokenLine, hex_to_int(TokenChars)}}.
@@ : {token, {qualifier, TokenLine, "@@"}}.
--.* : skip_token.


Erlang code.
strip(TokenChars, TokenLen) ->
    lists:sublist(TokenChars, 2, TokenLen - 2).

hex_to_int([_, _ | R]) ->
    {ok, [Int], []} = io_lib:fread("~16u", R),
    Int.

keyword_or_var(TokenChars, TokenLine) ->
    Atom = erlang:list_to_atom(string:to_lower(TokenChars)),
    case is_keyword(Atom) of
    true -> {Atom, TokenLine, {Atom, TokenChars}};
    false -> {var, TokenLine, TokenChars}
    end.

is_keyword(create) -> true;
is_keyword(table) -> true;
is_keyword('not') -> true;
is_keyword(null) -> true;
is_keyword(default) -> true;
is_keyword(signed) -> true;
is_keyword(unsigned) -> true;
is_keyword(comment) -> true;
is_keyword(charset) -> true;
is_keyword(engine) -> true;
is_keyword('if') -> true;
is_keyword(character) -> true;
is_keyword(set) -> true;
is_keyword(codec) -> true;
is_keyword(alias) -> true;
is_keyword(constraint) -> true;
is_keyword(primary) -> true;
is_keyword(key) -> true;
is_keyword(index) -> true;
is_keyword(using) -> true;
is_keyword(tinyint) -> true;
is_keyword(smallint) -> true;
is_keyword(int) -> true;
is_keyword(bigint) -> true;
is_keyword(varchar) -> true;
is_keyword(char) -> true;
is_keyword(text) -> true;
is_keyword(storage) -> true;
is_keyword(collate) -> true;
is_keyword(tinyblob) -> true;
is_keyword(blob) -> true;
is_keyword(mediumblob) -> true;
is_keyword(longblob) -> true;
is_keyword(drop) -> true;
is_keyword(exists) -> true;
is_keyword(names) -> true;
is_keyword(global) -> true;
is_keyword(session) -> true;
is_keyword(local) -> true;
is_keyword(auto_increment) -> true;
is_keyword(unique) -> true;
is_keyword(_) -> false.