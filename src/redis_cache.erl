%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <meituan>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(redis_cache).

-export([start/0, stop/0]).

-export([start_link/1]).

-export([put_val/1,
         put_val/3,
         put_val/4,
         del_val/1,
         del_val/2,
         set_val/1,
         get_val/2,
         get_val/3]).

-export([purge/0,
         clean/0,
         clean/1,
         reload/0,
         reload/1,
         get_redis_notice_len/0,
         get_cache_notice_len/0,
         get_table_vals/1,
         get_redis_vals/1]).

%% callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-define(TIMEOUT, 500).

-record(state, {tables = [], notice_len = 0, reduce_max = 0, reduce_len = 0}).

-define(BIN(V), to_binary(V)).

-define(REDIS_TABLE(L), iolist_to_binary([<<"$redis_cache_kv_">> | L])).
-define(ETS_TABLE(L), list_to_atom("$redis_cache_kv_" ++ atom_to_list(L))).

-define(NOTICE, <<"$redis_cache_notice">>).

-define(REDUCE_MAX, 1000).
-define(REDUCE_LEN(X), (X div 4 * 3)).

%%------------------------------------------------------------------------------
start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

%%------------------------------------------------------------------------------
start_link(Tables) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Tables], []).

put_val(Table, Key, CKey, CVal) ->
    put_val(Table, Key, [{CKey, CVal}]).

%% [{ckey, cval}]
put_val(Table, Key, MKV) ->
    put_val([{Table, Key, MKV}]).

%% [{table, key, [{ckey, cval}]}]
put_val(List) ->
    set_val([{put_val, T, K, M} || {T, K, M} <- List]).

del_val(Table, Key) ->
    del_val([{Table, Key}]).

%% [{table, key}]
del_val(List) ->
    set_val([{del_val, T, K} || {T, K} <- List]).

%% [{op, ...}]
set_val(List) ->
    gen_server:call(?MODULE, {set_val, List}).

get_val(Table, Key) ->
    case ets:lookup(?ETS_TABLE(Table), Key) of
        [{Key, Map}] -> Map;
        _ -> null
    end.

get_val(Table, Key, CKey) ->
    case get_val(Table, Key) of
        null -> null;
        Map ->
            case maps:find(CKey, Map) of
                {ok, Val} -> Val;
                error -> null
            end
    end.

reload() -> reload('$all').
reload(Table) when is_atom(Table) -> reload([Table]);
reload(List) -> gen_server:call(?MODULE, {reload, List}).

clean() -> clean('$all').
clean(Table) when is_atom(Table) -> clean([Table]);
clean(List) -> gen_server:call(?MODULE, {clean, List}).

purge() ->
    {ok, Tables} = application:get_env(redis_cache, tables),
    {ok, _} = eredis_pool:q([<<"DEL">>, ?NOTICE]),
    do_clean_table(Tables), ok.

get_redis_notice_len() ->
    {ok, BLen} = eredis_pool:q([<<"LLEN">>, ?NOTICE]), binary_to_integer(BLen).

get_cache_notice_len() ->
    State = sys:get_state(?MODULE), State#state.notice_len.

get_table_vals(Table) ->
    ets:tab2list(?ETS_TABLE(Table)).

get_redis_vals(Table) ->
    {ok, KeyList} = eredis_pool:q([<<"KEYS">>, ?REDIS_TABLE([?BIN(Table), <<"*">>])]),
    [begin
         {ok, Vals} = eredis_pool:q([<<"HGETALL">>, ?REDIS_TABLE([?BIN(Table), "_", ?BIN(Key)])]),
         {Key, Vals}
     end || Key <- KeyList].

%%------------------------------------------------------------------------------
init([Tables]) ->
    Max = application:get_env(redis_cache, reduce_max, ?REDUCE_MAX),
    [do_load_table(Table) || Table <- Tables],
    {ok, #state{tables = Tables, reduce_max = Max, reduce_len = ?REDUCE_LEN(Max)}, 0}.

handle_call({set_val, List}, _From, State) ->
    {reply, catch do_set_val(List), State};
handle_call({reload, ['$all']}, _From, State) ->
    {reply, catch do_reload_table(State#state.tables), State};
handle_call({reload, List}, _From, State) ->
    {reply, catch do_reload_table(List), State};
handle_call({clean, ['$all']}, _From, State) ->
    {reply, catch do_clean_table(State#state.tables), State};
handle_call({clean, List}, _From, State) ->
    {reply, catch do_clean_table(List), State};
handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(timeout, State) ->
    erlang:send_after(?TIMEOUT, self(), timeout),
    case catch do_check_update(State) of
        {'EXIT', _} -> {noreply, State};
        NState -> {noreply, NState}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
to_binary(X) when is_list(X)    -> list_to_binary(X);
to_binary(X) when is_atom(X)    -> list_to_binary(atom_to_list(X));
to_binary(X) when is_integer(X) -> integer_to_binary(X);
to_binary(X) when is_binary(X)  -> X.

get_type(X) when is_binary(X) -> binary;
get_type(X) when is_list(X) -> list;
get_type(X) when is_integer(X) -> integer;
get_type(X) when is_atom(X) -> atom.

put_type(X, binary) -> X;
put_type(X, list) -> binary_to_list(X);
put_type(X, integer) -> binary_to_integer(X);
put_type(X, atom) -> list_to_atom(binary_to_list(X)).

encode(V) ->
    jsx:encode(#{<<"type">> => get_type(V), <<"val">> => to_binary(V)}).

decode(B) ->
    #{<<"type">> := T, <<"val">> := V} = jsx:decode(B, [return_maps]),
    put_type(V, put_type(T, atom)).

%%------------------------------------------------------------------------------
do_load_table(Table) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:new(EtsTable, [named_table, public, {read_concurrency, true}]),
    {ok, List} = eredis_pool:q([<<"KEYS">>, ?REDIS_TABLE([?BIN(Table), <<"*">>])]),
    [do_load_table_key(EtsTable, RedisTable) || RedisTable <- List].

do_load_table_key(EtsTable, RedisTable) ->
    {ok, List} = eredis_pool:q([<<"HGETALL">>, RedisTable]),
    {_Table, EtsKey} = do_parse_redis_table(RedisTable),
    ets:insert(EtsTable, {EtsKey, do_form_map(List, maps:new())}).

do_form_map([Key, Val | T], Map) ->
    do_form_map(T, Map#{decode(Key) => decode(Val)});
do_form_map([], Map) -> Map.

do_reload_table([Table | T]) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:delete_all_objects(EtsTable),
    {ok, List} = eredis_pool:q([<<"KEYS">>, ?REDIS_TABLE([?BIN(Table), <<"*">>])]),
    [do_load_table_key(EtsTable, RedisTable) || RedisTable <- List],
    do_reload_table(T);
do_reload_table([]) -> ok.

do_clean_table([Table | T]) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:info(EtsTable) =/= undefined andalso ets:delete_all_objects(EtsTable),
    {ok, List} = eredis_pool:q([<<"KEYS">>, ?REDIS_TABLE([?BIN(Table), <<"*">>])]),
    [{ok, _} = eredis_pool:q([<<"DEL">>, RedisTable]) || RedisTable <- List],
    do_clean_table(T);
do_clean_table([]) -> ok.

%%------------------------------------------------------------------------------
do_set_val([]) -> ok;
do_set_val(List) ->
    do_set_redis(List, []),
    do_set_ets(List), ok.

do_set_redis([{put_val, Table, Key, MKV} | T], Acc) ->
    do_set_redis(T, do_form_put(Table, Key, MKV, []) ++ Acc);
do_set_redis([{del_val, Table, Key} | T], Acc) ->
    do_set_redis(T, do_form_del(Table, Key) ++ Acc);
do_set_redis([], Acc) -> {ok, _} = eredis_pool:transaction(Acc).

do_form_put(Table, Key, [{CK, CV} | T], Acc) ->
    do_form_put(Table, Key, T, [encode(CK), encode(CV)| Acc]);
do_form_put(_Table, _Key, [], []) -> [];
do_form_put(Table, Key, [], Acc) ->
    Key1 = ?REDIS_TABLE([?BIN(Table), "_", ?BIN(Key)]),
    [[<<"LPUSH">>, ?NOTICE, jsx:encode(#{<<"op">> => <<"put">>, <<"table">> => Key1})],
     [<<"HMSET">>, Key1] ++ Acc].

do_form_del(Table, Key) ->
    Key1 = ?REDIS_TABLE([?BIN(Table), "_", ?BIN(Key)]),
    [[<<"LPUSH">>, ?NOTICE, jsx:encode(#{<<"op">> => <<"del">>, <<"table">> => Key1})],
     [<<"DEL">>, ?REDIS_TABLE([?BIN(Table), "_", ?BIN(Key)])]].

do_set_ets([{put_val, Table, Key, MKV} | T]) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:insert(EtsTable, {Key, do_get_map(EtsTable, Key, MKV)}),
    do_set_ets(T);
do_set_ets([{del_val, Table, Key} | T]) ->
    ets:delete(?ETS_TABLE(Table), Key),
    do_set_ets(T);
do_set_ets([]) -> skip.

do_get_map(Name, Key, MKV) ->
    case ets:lookup(Name, Key) of
        [] -> maps:from_list(MKV);
        [{_Key, Map}] -> maps:merge(Map, maps:from_list(MKV))
    end.

%%------------------------------------------------------------------------------
do_check_update(#state{notice_len = Len, reduce_max = Max, reduce_len = RLen} = State) ->
    case eredis_pool:q([<<"LLEN">>, ?NOTICE]) of
        {ok, NBinLen} ->
            case binary_to_integer(NBinLen) of
                NLen when NLen >= Max ->
                    do_reduce_len(NLen, RLen), State;
                NLen when NLen > Len ->
                    do_update_len(Len, NLen),
                    State#state{notice_len = NLen};
                NLen when NLen < Len andalso NLen > Len rem RLen ->
                    do_update_len(Len rem RLen, NLen),
                    State#state{notice_len = NLen};
                NLen ->
                    State#state{notice_len = NLen}
            end;
        _ ->
            State
    end.

do_reduce_len(Len, RLen) ->
    Reduce = Len div RLen * RLen,
    {ok, _} = eredis_pool:q([<<"LTRIM">>, ?NOTICE, Reduce, <<"-1">>]).

do_update_len(Len, NLen) ->
    {ok, List} = eredis_pool:q([<<"LRANGE">>, ?NOTICE, Len, NLen - 1]),
    [do_update_key(Val) || Val  <- List].

do_update_key(Val) ->
    #{<<"op">> := OP, <<"table">> := RedisTable} =jsx:decode(Val, [return_maps]),
    {Table, EtsKey} = do_parse_redis_table(RedisTable),
    EtsTable = ?ETS_TABLE(put_type(Table, atom)),
    case OP of
        <<"put">> ->
            {ok, List} = eredis_pool:q([<<"HGETALL">>, RedisTable]),
            ets:insert(EtsTable, {EtsKey, do_form_map(List, maps:new())});
        <<"del">> ->
            ets:delete(EtsTable, EtsKey)
    end.

do_parse_redis_table(RedisTable) ->
    [<<>>, TK] = binary:split(RedisTable, ?REDIS_TABLE(<<>>)),
    [Table, EtsKey] = re:split(TK, <<"_">>),
    {Table, put_type(EtsKey, atom)}.

